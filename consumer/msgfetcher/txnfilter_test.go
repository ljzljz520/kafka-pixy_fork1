package msgfetcher

import (
	"encoding/binary"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/config"
	. "gopkg.in/check.v1"
)

// makeRecords builds a record batch with the given number of records, each
// record offset delta being its index. A batch is the transaction visibility
// unit: a multi-record batch is either fully visible or fully filtered.
func makeRecords(firstOffset, producerID int64, isTransactional, isControl bool, count int) *sarama.RecordBatch {
	batch := &sarama.RecordBatch{
		Version:         2,
		FirstOffset:     firstOffset,
		LastOffsetDelta: int32(count - 1),
		ProducerID:      producerID,
		IsTransactional: isTransactional,
		Control:         isControl,
	}
	for i := 0; i < count; i++ {
		batch.Records = append(batch.Records, &sarama.Record{
			OffsetDelta: int64(i),
			Key:         nil,
			Value:       []byte("v"),
		})
	}
	return batch
}

// makeControlRecordBatch builds a transaction control batch with a properly
// encoded control record key (version int16 + type int16).
func makeControlRecordBatch(firstOffset, producerID int64, abort bool) *sarama.RecordBatch {
	batch := makeRecords(firstOffset, producerID, true, true, 1)
	key := make([]byte, 4)
	if !abort {
		binary.BigEndian.PutUint16(key[2:4], 1) // commit
	}
	batch.Records[0].Key = key
	return batch
}

// addBatch appends a prebuilt record batch to a fetch response block.
func addBatch(resp *sarama.FetchResponse, topic string, partition int32, batch *sarama.RecordBatch) {
	block := resp.GetBlock(topic, partition)
	if block == nil {
		resp.AddError(topic, partition, sarama.ErrNoError)
		block = resp.GetBlock(topic, partition)
	}
	block.RecordsSet = append(block.RecordsSet, &sarama.Records{RecordBatch: batch})
}

func emptyV4Block() *sarama.FetchResponse {
	resp := &sarama.FetchResponse{Version: 4}
	resp.AddError("my_topic", 0, sarama.ErrNoError)
	return resp
}

// White-box classification of batches against cross-fetch transaction state:
// committed/aborted/pending-free decisions, control batches, marker ordering,
// stale markers and producer ID reuse after an abort.
func (s *MsgFetcherSuite) TestTxnFilterClassify(c *C) {
	tf := newTxnFilter(s.ns)
	block := &sarama.FetchResponseBlock{
		LastStableOffset: 24,
		// Markers arrive unsorted; beginBlock must order them by FirstOffset.
		AbortedTransactions: []*sarama.AbortedTransaction{
			{ProducerID: 9, FirstOffset: 20},
			{ProducerID: 7, FirstOffset: 10},
		},
	}
	tf.beginBlock(block)
	c.Assert(tf.lastStableOffset, Equals, int64(24))
	c.Assert(tf.aborted, DeepEquals, []abortedTxn{
		{producerID: 7, firstOffset: 10},
		{producerID: 9, firstOffset: 20},
	})

	expect := func(batch *sarama.RecordBatch, wantVisible bool, wantSpan Span) {
		visible, span := tf.classifyBatch(batch)
		c.Assert(visible, Equals, wantVisible)
		if !wantVisible {
			c.Assert(span, DeepEquals, wantSpan)
		}
	}

	// A regular non-transactional batch below the first marker is visible.
	expect(makeRecords(0, -1, false, false, 1), true, Span{})

	// Transactional batch of producer 7 starting at its abort FirstOffset is
	// filtered as a whole, including multi-record batches.
	expect(makeRecords(10, 7, true, false, 1), false, Span{From: 10, To: 11})
	expect(makeRecords(20, 9, true, false, 2), false, Span{From: 20, To: 22})

	// Abort control batches close the aborted intervals of both producers.
	expect(makeControlRecordBatch(22, 9, true), false, Span{From: 22, To: 23})
	expect(makeControlRecordBatch(23, 7, true), false, Span{From: 23, To: 24})
	c.Assert(tf.abortedPIDs, HasLen, 0)
	c.Assert(tf.completedAborts[7], Equals, int64(24))
	c.Assert(tf.completedAborts[9], Equals, int64(23))

	// A commit control batch does not reopen anything: a producer whose
	// abort already finished is unaffected by a later commit marker of a
	// subsequent committed transaction (control batches are filtered but
	// never abort).
	expect(makeControlRecordBatch(29, 7, false), false, Span{From: 29, To: 30})

	// A later committed transaction of the same producer (epoch bumped) is
	// visible.
	expect(makeRecords(30, 7, true, false, 1), true, Span{})

	// A stale marker re-sent for the finished abort is rejected; a marker of
	// a new aborted transaction of the same producer is accepted and clears
	// the obsolete guard.
	tf.addAborted(7, 10)
	c.Assert(tf.aborted, HasLen, 0)
	tf.addAborted(7, 35)
	// Duplicate markers are merged.
	tf.addAborted(7, 35)
	// Only producer 7's guard is obsolete; producer 9's completed abort is
	// still tracked.
	c.Assert(tf.completedAborts, HasLen, 1)
	c.Assert(tf.aborted, DeepEquals, []abortedTxn{{producerID: 7, firstOffset: 35}})
	expect(makeRecords(35, 7, true, false, 1), false, Span{From: 35, To: 36})
	c.Assert(tf.aborted, HasLen, 0)
}

// readCommittedEnv prepares the mock broker wiring used by read_committed
// fetcher tests. Unbuffered channels make the interleaving of messages and
// filtered spans deterministic: the fetcher blocks until each stream item is
// consumed.
func (s *MsgFetcherSuite) readCommittedEnv(c *C) {
	s.cfg.Kafka.Version.Set(sarama.V0_11_0_0)
	s.cfg.Consumer.Isolation = config.ReadCommittedIsolation
	s.cfg.Consumer.ChannelBufferSize = 0
}

type streamExpectation struct {
	isSpan bool
	offset int64 // expected message offset
	from   int64
	to     int64
}

func expectStream(c *C, mf T, want streamExpectation) {
	concrete := mf.(*msgFetcher)
	select {
	case span := <-mf.FilteredRanges():
		if !want.isSpan {
			c.Fatalf("got span %v, want message at %d", span, want.offset)
		}
		c.Assert(span, DeepEquals, Span{From: want.from, To: want.to})
	case msg := <-mf.Messages():
		if want.isSpan {
			c.Fatalf("got message at %d, want span [%d,%d)", msg.Offset, want.from, want.to)
		}
		c.Assert(msg.Offset, Equals, want.offset)
	case err := <-concrete.errorsCh:
		c.Fatalf("unexpected fetcher error: %v", err)
	case <-time.After(2 * time.Second):
		c.Fatal("timeout waiting for stream item")
	}
}

// Committed transaction data is delivered while aborted transaction data and
// control batches are filtered. The fetch request itself carries the
// ReadCommitted isolation level. Producer ID reuse after an abort delivers
// again.
func (s *MsgFetcherSuite) TestReadCommittedFetch(c *C) {
	s.readCommittedEnv(c)

	resp := &sarama.FetchResponse{
		Version: 4,
		Blocks: map[string]map[int32]*sarama.FetchResponseBlock{"my_topic": {0: {
			LastStableOffset:    1240,
			AbortedTransactions: []*sarama.AbortedTransaction{{ProducerID: 7, FirstOffset: 1235}},
		}}},
	}
	// Committed record of producer 7 preceding the aborted transaction.
	resp.AddRecordBatch("my_topic", 0, nil, testMsg, 1234, 7, true)
	// Two aborted records.
	resp.AddRecordBatch("my_topic", 0, nil, testMsg, 1235, 7, true)
	resp.AddRecordBatch("my_topic", 0, nil, testMsg, 1236, 7, true)
	// Abort control batch.
	resp.AddControlRecord("my_topic", 0, 1237, 7, sarama.ControlRecordAbort)
	// Committed records of the same producer after the abort, followed by a
	// non-transactional record.
	resp.AddRecordBatch("my_topic", 0, nil, testMsg, 1238, 7, true)
	resp.AddRecordBatch("my_topic", 0, nil, testMsg, 1239, -1, false)

	s.broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(s.broker0.Addr(), s.broker0.BrokerID()).
			SetLeader("my_topic", 0, s.broker0.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(c).
			SetVersion(1).
			SetOffset("my_topic", 0, sarama.OffsetOldest, 0).
			SetOffset("my_topic", 0, offsetLastStable, 1240),
		"FetchRequest": sarama.NewMockSequence(resp, emptyV4Block()),
	})

	kafkaClt, _ := sarama.NewClient([]string{s.broker0.Addr()}, s.cfg.SaramaClientCfg())
	defer kafkaClt.Close()

	f := SpawnFactory(s.ns, s.cfg, kafkaClt)
	defer f.Stop()

	mf, concreteOffset, err := f.Spawn(s.ns.NewChild("my_topic", 0), "my_topic", 0, 1234)
	c.Assert(err, IsNil)
	defer mf.Stop()
	c.Assert(concreteOffset, Equals, int64(1234))

	// Stream order: committed message, two aborted spans, control span,
	// committed messages.
	expectStream(c, mf, streamExpectation{offset: 1234})
	expectStream(c, mf, streamExpectation{isSpan: true, from: 1235, to: 1236})
	expectStream(c, mf, streamExpectation{isSpan: true, from: 1236, to: 1237})
	expectStream(c, mf, streamExpectation{isSpan: true, from: 1237, to: 1238})
	expectStream(c, mf, streamExpectation{offset: 1238})
	expectStream(c, mf, streamExpectation{offset: 1239})

	// The broker saw a v4 fetch request with read_committed isolation.
	var found bool
	for _, rr := range s.broker0.History() {
		if fr, ok := rr.Request.(*sarama.FetchRequest); ok {
			c.Assert(fr.Version, Equals, int16(4))
			c.Assert(fr.Isolation, Equals, sarama.ReadCommitted)
			found = true
			break
		}
	}
	c.Assert(found, Equals, true)
}

// Transaction state survives across fetch responses: an aborted batch learned
// from one response stays filtered, and the abort control batch arriving in a
// later response (without a repeated marker) clears the producer so its next
// committed transaction is visible. A response containing only the aborted
// batch keeps the scan going.
func (s *MsgFetcherSuite) TestReadCommittedCrossFetch(c *C) {
	s.readCommittedEnv(c)

	// Response 1: aborted data batch with its marker, no control batch yet.
	resp1 := &sarama.FetchResponse{
		Version: 4,
		Blocks: map[string]map[int32]*sarama.FetchResponseBlock{"my_topic": {0: {
			LastStableOffset:    1,
			AbortedTransactions: []*sarama.AbortedTransaction{{ProducerID: 5, FirstOffset: 0}},
		}}},
	}
	resp1.AddRecordBatch("my_topic", 0, nil, testMsg, 0, 5, true)

	// Response 2: the abort control batch WITHOUT repeating the marker, then
	// a committed batch of the same producer.
	resp2 := &sarama.FetchResponse{Version: 4}
	resp2.AddControlRecord("my_topic", 0, 1, 5, sarama.ControlRecordAbort)
	resp2.AddRecordBatch("my_topic", 0, nil, testMsg, 2, 5, true)
	resp2.SetLastStableOffset("my_topic", 0, 3)

	s.broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(s.broker0.Addr(), s.broker0.BrokerID()).
			SetLeader("my_topic", 0, s.broker0.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(c).
			SetVersion(1).
			SetOffset("my_topic", 0, sarama.OffsetOldest, 0).
			SetOffset("my_topic", 0, offsetLastStable, 3),
		"FetchRequest": sarama.NewMockSequence(resp1, resp2, emptyV4Block()),
	})

	kafkaClt, _ := sarama.NewClient([]string{s.broker0.Addr()}, s.cfg.SaramaClientCfg())
	defer kafkaClt.Close()

	f := SpawnFactory(s.ns, s.cfg, kafkaClt)
	defer f.Stop()

	mf, _, err := f.Spawn(s.ns.NewChild("my_topic", 0), "my_topic", 0, 0)
	c.Assert(err, IsNil)
	defer mf.Stop()

	expectStream(c, mf, streamExpectation{isSpan: true, from: 0, to: 1})
	expectStream(c, mf, streamExpectation{isSpan: true, from: 1, to: 2})
	expectStream(c, mf, streamExpectation{offset: 2})
}

// A fetch response consisting of a single commit control batch produces no
// messages but still advances the scan cursor: the next response is fetched.
func (s *MsgFetcherSuite) TestControlOnlyResponse(c *C) {
	s.readCommittedEnv(c)

	resp1 := &sarama.FetchResponse{Version: 4}
	resp1.AddControlRecord("my_topic", 0, 0, 9, sarama.ControlRecordCommit)
	resp1.SetLastStableOffset("my_topic", 0, 1)

	resp2 := &sarama.FetchResponse{Version: 4}
	resp2.AddRecordBatch("my_topic", 0, nil, testMsg, 1, -1, false)
	resp2.SetLastStableOffset("my_topic", 0, 2)

	s.broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(s.broker0.Addr(), s.broker0.BrokerID()).
			SetLeader("my_topic", 0, s.broker0.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(c).
			SetVersion(1).
			SetOffset("my_topic", 0, sarama.OffsetOldest, 0).
			SetOffset("my_topic", 0, offsetLastStable, 2),
		"FetchRequest": sarama.NewMockSequence(resp1, resp2, emptyV4Block()),
	})

	kafkaClt, _ := sarama.NewClient([]string{s.broker0.Addr()}, s.cfg.SaramaClientCfg())
	defer kafkaClt.Close()

	f := SpawnFactory(s.ns, s.cfg, kafkaClt)
	defer f.Stop()

	mf, _, err := f.Spawn(s.ns.NewChild("my_topic", 0), "my_topic", 0, 0)
	c.Assert(err, IsNil)
	defer mf.Stop()

	expectStream(c, mf, streamExpectation{isSpan: true, from: 0, to: 1})
	expectStream(c, mf, streamExpectation{offset: 1})
}

// In read_committed mode a brand new group starting at the newest position
// uses the last stable offset (ListOffsets timestamp sentinel -3), not the
// physical log end offset that could run past an open transaction.
func (s *MsgFetcherSuite) TestStartingOffsetIsLSO(c *C) {
	s.readCommittedEnv(c)

	s.broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(s.broker0.Addr(), s.broker0.BrokerID()).
			SetLeader("my_topic", 0, s.broker0.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(c).
			SetVersion(1).
			SetOffset("my_topic", 0, sarama.OffsetOldest, 0).
			SetOffset("my_topic", 0, offsetLastStable, 50),
		"FetchRequest": sarama.NewMockWrapper(emptyV4Block()),
	})

	kafkaClt, _ := sarama.NewClient([]string{s.broker0.Addr()}, s.cfg.SaramaClientCfg())
	defer kafkaClt.Close()

	f := SpawnFactory(s.ns, s.cfg, kafkaClt)
	defer f.Stop()

	mf, concreteOffset, err := f.Spawn(s.ns.NewChild("my_topic", 0), "my_topic", 0, sarama.OffsetNewest)
	c.Assert(err, IsNil)
	defer mf.Stop()

	c.Assert(concreteOffset, Equals, int64(50))

	// The mock offset response only answers the LSO sentinel (-3) and the
	// oldest query (-2): querying the physical log end (-1) would make the
	// mock reporter fail the test.
}

// In read_uncommitted mode transactional batches of aborted producers remain
// visible, control batches are silently ignored, no spans are reported and the
// fetch request carries ReadUncommitted isolation.
func (s *MsgFetcherSuite) TestReadUncommittedNoFiltering(c *C) {
	s.cfg.Kafka.Version.Set(sarama.V0_11_0_0)
	s.cfg.Consumer.Isolation = config.ReadUncommittedIsolation
	s.cfg.Consumer.ChannelBufferSize = 0

	resp := &sarama.FetchResponse{Version: 4}
	resp.AddRecordBatch("my_topic", 0, nil, testMsg, 0, 7, true)
	resp.AddRecordBatch("my_topic", 0, nil, testMsg, 1, 7, true)
	resp.AddControlRecord("my_topic", 0, 2, 7, sarama.ControlRecordAbort)
	resp.AddRecordBatch("my_topic", 0, nil, testMsg, 3, 7, true)

	s.broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(s.broker0.Addr(), s.broker0.BrokerID()).
			SetLeader("my_topic", 0, s.broker0.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(c).
			SetVersion(1).
			SetOffset("my_topic", 0, sarama.OffsetOldest, 0).
			SetOffset("my_topic", 0, sarama.OffsetNewest, 4),
		"FetchRequest": sarama.NewMockSequence(resp, emptyV4Block()),
	})

	kafkaClt, _ := sarama.NewClient([]string{s.broker0.Addr()}, s.cfg.SaramaClientCfg())
	defer kafkaClt.Close()

	f := SpawnFactory(s.ns, s.cfg, kafkaClt)
	defer f.Stop()

	mf, _, err := f.Spawn(s.ns.NewChild("my_topic", 0), "my_topic", 0, 0)
	c.Assert(err, IsNil)
	defer mf.Stop()

	// All data batches are delivered, including the aborted ones; the control
	// batch produces nothing.
	for _, want := range []int64{0, 1, 3} {
		select {
		case msg := <-mf.Messages():
			c.Assert(msg.Offset, Equals, want)
		case err := <-mf.(*msgFetcher).errorsCh:
			c.Fatal(err)
		case <-time.After(2 * time.Second):
			c.Fatal("timeout waiting for message")
		}
	}

	// No filtered span is ever reported in read_uncommitted mode.
	select {
	case span, ok := <-mf.FilteredRanges():
		if ok {
			c.Fatalf("unexpected span in read_uncommitted mode: %v", span)
		}
	case <-time.After(200 * time.Millisecond):
	}

	// The fetch request carries ReadUncommitted isolation.
	var found bool
	for _, rr := range s.broker0.History() {
		if fr, ok := rr.Request.(*sarama.FetchRequest); ok {
			c.Assert(fr.Version, Equals, int16(4))
			c.Assert(fr.Isolation, Equals, sarama.ReadUncommitted)
			found = true
			break
		}
	}
	c.Assert(found, Equals, true)
}
