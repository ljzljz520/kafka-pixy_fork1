package msgfetcher

import (
	"encoding/binary"
	"sort"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
)

// Control record type encoded in the last two bytes of the 4-byte control
// record key (version int16 followed by type int16, big-endian), see KIP-98.
const (
	controlRecordKeyLen = 4
	controlRecordAbort  = 0
	controlRecordCommit = 1
)

// txnFilter keeps the cross-fetch transaction state required to implement
// read_committed visibility on top of Fetch API v4 responses. One instance is
// owned by each msgFetcher, therefore the state is rebuilt from scratch by
// re-scanning Fetch responses starting from the committed checkpoint every
// time the fetcher is recreated (leader switch, fetcher failure, partition
// reassignment); stale transaction decisions are never carried over.
//
// The broker withholds all records of unresolved transactions beyond the last
// stable offset, so every transactional batch present in a read_committed
// Fetch response belongs either to a committed transaction (visible) or to an
// aborted one (must be dropped). The response block additionally carries:
//   - LastStableOffset, the upper bound of offsets decided by the protocol;
//   - AbortedTransactions, a producerID/firstOffset list of aborted
//     transactions whose data intersects the fetched range.
//
// The algorithm mirrors the Java client: aborted transaction markers are
// kept in a queue sorted by FirstOffset; when the scan reaches a batch whose
// last offset passes a marker, the producer ID joins the active aborted set;
// the set is cleared when the producer's abort control batch is observed.
// Unlike the Java client the state persists across Fetch responses here.
type txnFilter struct {
	actDesc *actor.Descriptor

	// lastStableOffset is the highest stable offset reported by the broker,
	// monotonically increasing across responses.
	lastStableOffset int64

	// aborted is a queue of aborted transaction markers, sorted by
	// firstOffset (producerID is used as a tie-breaker), that the scan
	// cursor has not reached yet.
	aborted []abortedTxn

	// abortedPIDs holds producer IDs whose transactional batches at the
	// current scan position are known aborted and must be dropped.
	abortedPIDs map[int64]struct{}

	// completedAborts maps a producer ID to the end offset of its already
	// scanned abort control batch. It rejects stale markers that a broker is
	// allowed to re-send for a fetch starting at the control batch end, so a
	// later committed transaction of the same producer (epoch bumped, offset
	// higher) cannot be filtered by mistake.
	completedAborts map[int64]int64
}

type abortedTxn struct {
	producerID  int64
	firstOffset int64
}

func newTxnFilter(actDesc *actor.Descriptor) *txnFilter {
	return &txnFilter{
		actDesc:         actDesc,
		abortedPIDs:     make(map[int64]struct{}),
		completedAborts: make(map[int64]int64),
	}
}

// beginBlock incorporates the transaction metadata of a new Fetch response
// block before its record batches are scanned.
func (tf *txnFilter) beginBlock(block *sarama.FetchResponseBlock) {
	if block.LastStableOffset > tf.lastStableOffset {
		tf.lastStableOffset = block.LastStableOffset
	}
	for _, at := range block.AbortedTransactions {
		tf.addAborted(at.ProducerID, at.FirstOffset)
	}
}

// addAborted merges an aborted transaction marker into the sorted pending
// queue dropping duplicates and stale markers of already finished aborts.
func (tf *txnFilter) addAborted(producerID, firstOffset int64) {
	// The abort control batch for this marker has already been scanned: the
	// marker is a stale retransmission, ignore it.
	if end, ok := tf.completedAborts[producerID]; ok {
		if firstOffset < end {
			return
		}
		// A marker at or beyond the end of the previous abort belongs to a
		// later transaction of the same producer, the old guard is obsolete.
		delete(tf.completedAborts, producerID)
	}
	i := sort.Search(len(tf.aborted), func(i int) bool {
		if tf.aborted[i].firstOffset != firstOffset {
			return tf.aborted[i].firstOffset > firstOffset
		}
		return tf.aborted[i].producerID >= producerID
	})
	if i < len(tf.aborted) &&
		tf.aborted[i].firstOffset == firstOffset &&
		tf.aborted[i].producerID == producerID {
		return
	}
	tf.aborted = append(tf.aborted, abortedTxn{})
	copy(tf.aborted[i+1:], tf.aborted[i:])
	tf.aborted[i] = abortedTxn{producerID: producerID, firstOffset: firstOffset}
}

// classifyBatch decides the fate of a record batch and updates the filter
// state. It returns visible=true when the batch records must be delivered to
// business clients; otherwise the returned half-open span [span.From,
// span.To) covers offsets proven non-deliverable (an aborted transaction data
// batch or a transaction control batch) and must be reported downstream so
// that the commit frontier can advance across them.
func (tf *txnFilter) classifyBatch(batch *sarama.RecordBatch) (visible bool, span Span) {
	lastOffset := batch.LastOffset()

	// Activate every marker whose transaction may start within or before
	// this batch. The queue is globally sorted by FirstOffset.
	for len(tf.aborted) > 0 && tf.aborted[0].firstOffset <= lastOffset {
		tf.abortedPIDs[tf.aborted[0].producerID] = struct{}{}
		tf.aborted = tf.aborted[1:]
	}

	span = Span{From: batch.FirstOffset, To: lastOffset + 1}

	// Control batches are never deliverable regardless of their type. An
	// abort control batch closes the aborted interval for its producer.
	if batch.Control {
		if controlRecordType(batch) == controlRecordAbort {
			pid := batch.ProducerID
			delete(tf.abortedPIDs, pid)
			tf.completedAborts[pid] = span.To
		}
		return false, span
	}

	// Transactional batches of known aborted producers are dropped as a
	// whole. Committed transactional batches and all non-transactional
	// batches are visible; the broker never returns unresolved data below
	// the last stable offset in read_committed mode.
	if batch.IsTransactional {
		if _, aborted := tf.abortedPIDs[batch.ProducerID]; aborted {
			return false, span
		}
	}
	return true, Span{}
}

// controlRecordType parses the transaction control record type out of a
// control record batch key. Returns -1 when the key cannot be parsed, in
// which case the batch is treated as a non-abort control batch.
func controlRecordType(batch *sarama.RecordBatch) int {
	if len(batch.Records) == 0 {
		return -1
	}
	key := batch.Records[0].Key
	if len(key) < controlRecordKeyLen {
		return -1
	}
	return int(binary.BigEndian.Uint16(key[2:4]))
}
