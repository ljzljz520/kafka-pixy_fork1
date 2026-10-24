package service

// End-to-end verification of the consumer.isolation=read_committed mode
// against a transaction capable Kafka cluster. The suite drives the gRPC
// ConsumeNAck API and the HTTP API against the same data and asserts that
// both protocol entry points expose exactly the same visible records:
// records of committed transactions and non-transactional records are
// delivered in offset order, records of aborted transactions and
// transaction control batches are never exposed, and records of unresolved
// transactions show up only after the transaction commits.
//
// Sarama v1.23.1 has no high level transactional producer API, therefore a
// minimal producer is implemented below on top of the raw KIP-98 requests
// (FindCoordinator/InitProducerID/AddPartitionsToTxn/Produce/EndTxn).

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path"
	"time"

	. "gopkg.in/check.v1"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/config"
	pb "github.com/mailgun/kafka-pixy/gen/golang"
	"github.com/mailgun/kafka-pixy/testhelpers"
	"github.com/mailgun/kafka-pixy/testhelpers/kafkahelper"
	"google.golang.org/grpc"
)

type ServiceTxnSuite struct {
	cfg        *config.App
	proxyCfg   *config.Proxy
	kh         *kafkahelper.T
	cltConn    *grpc.ClientConn
	clt        pb.KafkaPixyClient
	unixClient *http.Client

	topic   string
	testSeq int
}

var _ = Suite(&ServiceTxnSuite{})

func (s *ServiceTxnSuite) SetUpSuite(_ *C) {
	testhelpers.InitLogging()
}

func (s *ServiceTxnSuite) SetUpTest(c *C) {
	s.testSeq++
	s.topic = fmt.Sprintf("txn.isolation.%d.%d", time.Now().UnixNano(), s.testSeq)

	s.cfg = &config.App{Proxies: make(map[string]*config.Proxy)}
	s.cfg.GRPCAddr = "127.0.0.1:19097"
	s.cfg.TCPAddr = "127.0.0.1:19098"
	s.cfg.UnixAddr = path.Join(os.TempDir(), "kafka-pixy-txn.sock")
	s.proxyCfg = testhelpers.NewTestProxyCfg("pxyT_client_id")
	s.proxyCfg.Consumer.OffsetsCommitInterval = 50 * time.Millisecond
	// Read committed visibility requires the Fetch v4 wire protocol.
	s.proxyCfg.Kafka.Version.Set(sarama.V0_11_0_0)
	s.proxyCfg.Consumer.Isolation = config.ReadCommittedIsolation
	s.cfg.Proxies["pxyT"] = s.proxyCfg
	s.cfg.DefaultCluster = "pxyT"

	// Create the topic before the kafka helper caches broker metadata,
	// otherwise the negative "unknown topic" cache entry would linger for
	// the whole test run.
	s.createTopic(c)

	var err error
	s.cltConn, err = grpc.Dial(s.cfg.GRPCAddr, grpc.WithInsecure())
	c.Assert(err, IsNil)
	s.clt = pb.NewKafkaPixyClient(s.cltConn)

	os.Remove(s.cfg.UnixAddr)
	s.kh = kafkahelper.New(c)
	s.unixClient = testhelpers.NewUDSHTTPClient(s.cfg.UnixAddr)
}

func (s *ServiceTxnSuite) TearDownTest(_ *C) {
	_ = s.cltConn.Close()
	s.kh.Close()
}

func (s *ServiceTxnSuite) createTopic(c *C) {
	scfg := testhelpers.NewTestProxyCfg("txn-admin").SaramaClientCfg()
	scfg.Version = sarama.V0_11_0_0
	adm, err := sarama.NewClusterAdmin(testhelpers.KafkaPeers, scfg)
	c.Assert(err, IsNil)
	defer adm.Close()
	err = adm.CreateTopic(s.topic, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)
	c.Assert(err, IsNil)

	// Wait until the leader of the single partition is discoverable.
	clt, err := sarama.NewClient(testhelpers.KafkaPeers, scfg)
	c.Assert(err, IsNil)
	defer clt.Close()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if leaders, err := clt.Leader(s.topic, 0); err == nil && leaders != nil {
			return
		}
		_ = clt.RefreshMetadata(s.topic)
		time.Sleep(100 * time.Millisecond)
	}
	c.Errorf("topic %s did not become available", s.topic)
}

func (s *ServiceTxnSuite) waitSvcUp(c *C, timeout time.Duration) {
	start := time.Now()
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := s.clt.GetOffsets(ctx, &pb.GetOffsetsRq{Topic: s.topic, Group: "test"})
		cancel()
		if err == nil {
			return
		}
		if time.Since(start) > timeout {
			c.Errorf("service is not up")
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// startAtOldest commits the OffsetOldest sentinel for the group so that the
// partition consumer starts scanning from the very first record.
func (s *ServiceTxnSuite) startAtOldest(c *C, group string) {
	s.kh.SetOffsetValues(group, s.topic, []int64{sarama.OffsetOldest})
}

// waitForLEO blocks until the partition log end offset reaches want. A
// successful EndTxn response does not imply that the commit/abort control
// batch is already readable: on a freshly created topic the transaction
// coordinator may first queue the marker for an unknown leader destination
// and retry it on its next poller tick, so the marker append can race
// records produced right after endTxn(). Read_committed consumers tolerate
// that via the last stable offset; tests must synchronize on it explicitly.
func (s *ServiceTxnSuite) waitForLEO(c *C, want int64) {
	deadline := time.Now().Add(10 * time.Second)
	for {
		if got := s.kh.GetNewestOffsets(s.topic)[0]; got == want {
			return
		} else if got > want {
			c.Fatalf("LEO for %s is %d, want %d", s.topic, got, want)
		}
		if time.Now().After(deadline) {
			c.Fatalf("LEO for %s is %d, want %d", s.topic,
				s.kh.GetNewestOffsets(s.topic)[0], want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (s *ServiceTxnSuite) grpcConsume(c *C, group string, autoAck bool) *pb.ConsRs {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := s.clt.ConsumeNAck(ctx, &pb.ConsNAckRq{
		Topic:   s.topic,
		Group:   group,
		AutoAck: autoAck,
	})
	c.Assert(err, IsNil, Commentf("group=%s", group))
	return res
}

// grpcConsumeNoAck fetches a message leaving it unacknowledged; the returned
// ack values are used to acknowledge it later.
func (s *ServiceTxnSuite) grpcConsumeNoAck(c *C, group string) *pb.ConsRs {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := s.clt.ConsumeNAck(ctx, &pb.ConsNAckRq{
		Topic: s.topic,
		Group: group,
		NoAck: true,
	})
	c.Assert(err, IsNil)
	return res
}

// grpcConsumeMustBlock asserts that no record is deliverable within a short
// client deadline: a record that must not be visible (or that does not exist
// yet) must never reach the client.
func (s *ServiceTxnSuite) grpcConsumeMustBlock(c *C, group string) {
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	res, err := s.clt.ConsumeNAck(ctx, &pb.ConsNAckRq{
		Topic:   s.topic,
		Group:   group,
		AutoAck: true,
	})
	if err == nil {
		c.Errorf("unexpected visible record for group=%s: %q", group, string(res.Message))
	}
}

func (s *ServiceTxnSuite) grpcAck(c *C, group string, partition int32, offset int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := s.clt.Ack(ctx, &pb.AckRq{
		Topic:     s.topic,
		Group:     group,
		Partition: partition,
		Offset:    offset,
	})
	c.Assert(err, IsNil)
}

func (s *ServiceTxnSuite) httpConsume(c *C, group string) *pb.ConsRs {
	res, err := s.unixClient.Get(fmt.Sprintf("http://_/topics/%s/messages?group=%s", s.topic, group))
	c.Assert(err, IsNil)
	c.Assert(res.StatusCode, Equals, http.StatusOK, Commentf("group=%s", group))
	return ParseConsRes(c, res)
}

// httpConsumeMustBlock asserts that the HTTP long poll times out without a
// record.
func (s *ServiceTxnSuite) httpConsumeMustBlock(c *C, group string) {
	res, err := s.unixClient.Get(fmt.Sprintf("http://_/topics/%s/messages?group=%s", s.topic, group))
	c.Assert(err, IsNil)
	if res.StatusCode != http.StatusRequestTimeout {
		body := ParseJSONBody(c, res)
		c.Errorf("expected request timeout for group=%s, got status=%d body=%v",
			group, res.StatusCode, body)
	}
}

func (s *ServiceTxnSuite) expectCommitted(c *C, group string, want int64) {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if got := s.kh.GetCommittedOffsets(group, s.topic)[0].Val; got == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.Errorf("committed offset for group=%s is %d, want %d",
		group, s.kh.GetCommittedOffsets(group, s.topic)[0].Val, want)
}

// Both protocol entry points expose exactly the same records: non
// transactional records and committed transaction records are visible,
// aborted transaction records are invisible, and a transaction spanning
// several batches is delivered record by record. The commit frontier reaches
// the physical log end offset (which includes aborted data and control
// batches) even though only five business messages were acknowledged.
func (s *ServiceTxnSuite) TestReadCommittedVisibleRecordsBothProtocols(c *C) {
	s.kh.PutMessages("plain", s.topic, map[string]int{"K": 1})

	committed1 := newTxnProducer(c, fmt.Sprintf("commit-a-%d", s.testSeq))
	committed1.begin(c, s.topic)
	committed1.send(c, s.topic, 0, "commit-1")
	committed1.endTxn(c, true)
	committed1.close()
	// Offsets 1 (data) and 2 (commit control batch).
	s.waitForLEO(c, 3)

	aborted := newTxnProducer(c, fmt.Sprintf("abort-b-%d", s.testSeq))
	aborted.begin(c, s.topic)
	aborted.send(c, s.topic, 0, "abort-1")
	aborted.endTxn(c, false)
	aborted.close()
	// Offsets 3 (aborted data) and 4 (abort control batch).
	s.waitForLEO(c, 5)

	s.kh.PutMessages("pl2", s.topic, map[string]int{"K": 1})

	// A committed transaction whose records are sent in two separate
	// batches (producer sequence numbers 0 and 1).
	committed2 := newTxnProducer(c, fmt.Sprintf("commit-c-%d", s.testSeq))
	committed2.begin(c, s.topic)
	committed2.send(c, s.topic, 0, "commit-2")
	committed2.send(c, s.topic, 0, "commit-3")
	committed2.endTxn(c, true)
	committed2.close()
	// Offset 5 is plain pl2, offsets 6/7 are committed data and 8 is the
	// commit control batch.
	s.waitForLEO(c, 9)

	want := []string{"plain:K:0", "commit-1", "pl2:K:0", "commit-2", "commit-3"}
	physicalEnd := s.kh.GetNewestOffsets(s.topic)[0]
	// The physical log end must account for filtered records and control
	// batches, otherwise this test would not prove anything.
	c.Assert(physicalEnd, Equals, int64(9))

	grpcGroup := fmt.Sprintf("g-grpc-%d", s.testSeq)
	httpGroup := fmt.Sprintf("g-http-%d", s.testSeq)
	s.startAtOldest(c, grpcGroup)
	s.startAtOldest(c, httpGroup)

	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	s.waitSvcUp(c, 5*time.Second)

	grpcGot := make([]string, len(want))
	for i := range want {
		grpcGot[i] = string(s.grpcConsume(c, grpcGroup, true).Message)
	}
	httpGot := make([]string, len(want))
	for i := range want {
		httpGot[i] = string(s.httpConsume(c, httpGroup).Message)
	}
	c.Check(grpcGot, DeepEquals, want)
	c.Check(httpGot, DeepEquals, want)

	svc.Stop()

	// Five visible messages were acknowledged on a nine offset log: spans
	// proved non-deliverable advanced the commit frontier over the aborted
	// records and all control batches for both entries.
	s.expectCommitted(c, grpcGroup, physicalEnd)
	s.expectCommitted(c, httpGroup, physicalEnd)
}

// Records of an unresolved transaction are invisible through both protocol
// entry points, but once the transaction commits they become visible without
// skipping anything.
func (s *ServiceTxnSuite) TestReadCommittedPendingThenCommitted(c *C) {
	s.kh.PutMessages("pre", s.topic, map[string]int{"K": 1})

	pending := newTxnProducer(c, fmt.Sprintf("pending-%d", s.testSeq))
	pending.begin(c, s.topic)
	pending.send(c, s.topic, 0, "pending-value")

	grpcGroup := fmt.Sprintf("g-grpc-%d", s.testSeq)
	httpGroup := fmt.Sprintf("g-http-%d", s.testSeq)
	s.startAtOldest(c, grpcGroup)
	s.startAtOldest(c, httpGroup)

	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	s.waitSvcUp(c, 5*time.Second)

	c.Check(string(s.grpcConsume(c, grpcGroup, true).Message), Equals, "pre:K:0")
	c.Check(string(s.httpConsume(c, httpGroup).Message), Equals, "pre:K:0")

	// While the transaction is open neither entry point can see its record.
	s.grpcConsumeMustBlock(c, grpcGroup)
	s.httpConsumeMustBlock(c, httpGroup)

	// Commit the transaction; the withheld record must now appear.
	pending.endTxn(c, true)
	pending.close()
	// Offset 1 is the formerly pending data, offset 2 its control batch.
	s.waitForLEO(c, 3)

	c.Check(string(s.grpcConsume(c, grpcGroup, true).Message), Equals, "pending-value")
	c.Check(string(s.httpConsume(c, httpGroup).Message), Equals, "pending-value")

	svc.Stop()
	s.expectCommitted(c, grpcGroup, s.kh.GetNewestOffsets(s.topic)[0])
	s.expectCommitted(c, httpGroup, s.kh.GetNewestOffsets(s.topic)[0])
}

// The commit frontier must never jump over an unacknowledged visible
// message even when aborted records and control batches follow it. After the
// message is acknowledged scanning resumes, aborted records stay invisible
// and later committed records are delivered. Restarting the proxy
// redelivers the unacknowledged record (at-least-once), never an aborted
// record.
func (s *ServiceTxnSuite) TestReadCommittedUnackedNotSkippedAndRestart(c *C) {
	s.kh.PutMessages("hold", s.topic, map[string]int{"K": 1})

	aborted := newTxnProducer(c, fmt.Sprintf("abort-%d", s.testSeq))
	aborted.begin(c, s.topic)
	aborted.send(c, s.topic, 0, "aborted-value")
	aborted.endTxn(c, false)
	aborted.close()
	// Offset 1 is the aborted data, offset 2 the abort control batch.
	s.waitForLEO(c, 3)

	committed := newTxnProducer(c, fmt.Sprintf("commit-%d", s.testSeq))
	committed.begin(c, s.topic)
	committed.send(c, s.topic, 0, "committed-after")
	committed.endTxn(c, true)
	committed.close()
	// Offset 3 is the committed data, offset 4 the commit control batch.
	s.waitForLEO(c, 5)

	physicalEnd := s.kh.GetNewestOffsets(s.topic)[0]
	c.Assert(physicalEnd, Equals, int64(5))

	group := fmt.Sprintf("g-restart-%d", s.testSeq)
	s.startAtOldest(c, group)

	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	s.waitSvcUp(c, 5*time.Second)

	// Fetch the first visible record and deliberately do not acknowledge
	// it while the fetcher scans the aborted interval behind it.
	first := s.grpcConsumeNoAck(c, group)
	c.Check(string(first.Message), Equals, "hold:K:0")
	c.Check(first.Offset, Equals, int64(0))
	time.Sleep(1500 * time.Millisecond)
	c.Check(s.kh.GetCommittedOffsets(group, s.topic)[0].Val, Equals, int64(0),
		Commentf("commit frontier must not cross the unacknowledged record"))

	// Acknowledging the held record lets the frontier merge the filtered
	// spans; the next visible record is the committed one, never the
	// aborted one.
	s.grpcAck(c, group, first.Partition, first.Offset)
	second := s.grpcConsumeNoAck(c, group)
	c.Check(string(second.Message), Equals, "committed-after")
	c.Check(second.Offset, Equals, int64(3))

	// Restart the proxy leaving the second record unacknowledged. The
	// filter state is rebuilt from the committed checkpoint and the record
	// is redelivered at least once.
	svc.Stop()
	svc, err = Spawn(s.cfg)
	c.Assert(err, IsNil)
	s.waitSvcUp(c, 5*time.Second)
	redelivered := s.grpcConsumeNoAck(c, group)
	c.Check(string(redelivered.Message), Equals, "committed-after")
	c.Check(redelivered.Offset, Equals, int64(3))

	s.grpcAck(c, group, redelivered.Partition, redelivered.Offset)
	svc.Stop()
	s.expectCommitted(c, group, physicalEnd)
}

// A brand new group (no committed offset at all) created while a
// transaction is still open must start at the last stable offset, not at the
// physical log end; otherwise the records of that transaction would be
// skipped once it commits.
func (s *ServiceTxnSuite) TestReadCommittedNewGroupStartsAtLSO(c *C) {
	s.kh.PutMessages("lsopre", s.topic, map[string]int{"K": 2})

	committed := newTxnProducer(c, fmt.Sprintf("lso-commit-%d", s.testSeq))
	committed.begin(c, s.topic)
	committed.send(c, s.topic, 0, "lso-committed")
	committed.endTxn(c, true)
	committed.close()
	// Offset 2 is the committed data, offset 3 its control batch.
	s.waitForLEO(c, 4)

	pending := newTxnProducer(c, fmt.Sprintf("lso-pending-%d", s.testSeq))
	pending.begin(c, s.topic)
	pending.send(c, s.topic, 0, "lso-pending")

	// No offset is committed for either group, the proxy subscribes them
	// for the first time while the transaction is unresolved.
	grpcGroup := fmt.Sprintf("g-lso-grpc-%d", s.testSeq)
	httpGroup := fmt.Sprintf("g-lso-http-%d", s.testSeq)

	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	s.waitSvcUp(c, 5*time.Second)

	s.grpcConsumeMustBlock(c, grpcGroup)
	s.httpConsumeMustBlock(c, httpGroup)

	pending.endTxn(c, true)
	pending.close()
	// Offset 4 is the formerly pending data, offset 5 its control batch.
	s.waitForLEO(c, 5)

	// With a physical-log-end starting position the pending record would
	// have been skipped forever.
	c.Check(string(s.grpcConsume(c, grpcGroup, true).Message), Equals, "lso-pending")
	c.Check(string(s.httpConsume(c, httpGroup).Message), Equals, "lso-pending")

	svc.Stop()
}

// txnProducer is a minimal KIP-98 transactional producer implemented on top
// of raw sarama broker requests. All partitions of a transaction are added
// with begin(); every send() appends one record in its own batch and bumps
// the per partition producer sequence; endTxn() makes the broker write the
// commit/abort control batch.
type txnProducer struct {
	clt   sarama.Client
	coord *sarama.Broker
	txnID string
	pid   int64
	epoch int16
	seq   map[string]int32
}

func newTxnProducer(c *C, txnID string) *txnProducer {
	scfg := testhelpers.NewTestProxyCfg("txnp-" + txnID).SaramaClientCfg()
	scfg.Version = sarama.V0_11_0_0
	clt, err := sarama.NewClient(testhelpers.KafkaPeers, scfg)
	c.Assert(err, IsNil)
	c.Assert(clt.Brokers(), HasLen, 1)
	broker := clt.Brokers()[0]
	if ok, _ := broker.Connected(); !ok {
		c.Assert(broker.Open(scfg), IsNil)
	}

	// The first transactional request of a cluster triggers creation of
	// the __transaction_state topic; the coordinator answers with
	// "coordinator not available"/"load in progress" meanwhile.
	var fc *sarama.FindCoordinatorResponse
	deadline := time.Now().Add(20 * time.Second)
	for {
		fc, err = broker.FindCoordinator(&sarama.FindCoordinatorRequest{
			Version:         1,
			CoordinatorKey:  txnID,
			CoordinatorType: sarama.CoordinatorTransaction,
		})
		c.Assert(err, IsNil)
		if fc.Err == sarama.ErrNoError {
			break
		}
		if fc.Err != sarama.ErrConsumerCoordinatorNotAvailable &&
			fc.Err != sarama.ErrNotCoordinatorForConsumer &&
			fc.Err != sarama.ErrOffsetsLoadInProgress {
			c.Fatalf("FindCoordinator failed: %v", fc.Err)
		}
		if time.Now().After(deadline) {
			c.Fatalf("transaction coordinator is not available: %v", fc.Err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	c.Assert(fc.Coordinator, NotNil)
	c.Assert(fc.Coordinator.Open(scfg), IsNil)

	id := txnID
	var init *sarama.InitProducerIDResponse
	initDeadline := time.Now().Add(20 * time.Second)
	for {
		init, err = fc.Coordinator.InitProducerID(&sarama.InitProducerIDRequest{
			TransactionalID:    &id,
			TransactionTimeout: 30 * time.Second,
		})
		c.Assert(err, IsNil)
		if init.Err == sarama.ErrNoError {
			break
		}
		if init.Err != sarama.ErrConsumerCoordinatorNotAvailable &&
			init.Err != sarama.ErrNotCoordinatorForConsumer &&
			init.Err != sarama.ErrOffsetsLoadInProgress {
			c.Fatalf("InitProducerID failed: %v", init.Err)
		}
		if time.Now().After(initDeadline) {
			c.Fatalf("transaction coordinator is not ready: %v", init.Err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	return &txnProducer{
		clt:   clt,
		coord: fc.Coordinator,
		txnID: txnID,
		pid:   init.ProducerID,
		epoch: init.ProducerEpoch,
		seq:   make(map[string]int32),
	}
}

func (p *txnProducer) begin(c *C, topic string) {
	resp, err := p.coord.AddPartitionsToTxn(&sarama.AddPartitionsToTxnRequest{
		TransactionalID: p.txnID,
		ProducerID:      p.pid,
		ProducerEpoch:   p.epoch,
		TopicPartitions: map[string][]int32{topic: {0}},
	})
	c.Assert(err, IsNil)
	errorsForTopic, ok := resp.Errors[topic]
	c.Assert(ok, Equals, true)
	c.Assert(errorsForTopic, HasLen, 1)
	c.Assert(errorsForTopic[0].Partition, Equals, int32(0))
	c.Assert(errorsForTopic[0].Err, Equals, sarama.ErrNoError)
}

func (p *txnProducer) leader(c *C, topic string, partition int32) *sarama.Broker {
	deadline := time.Now().Add(10 * time.Second)
	for {
		leader, err := p.clt.Leader(topic, partition)
		if err == nil && leader != nil {
			if ok, _ := leader.Connected(); ok {
				return leader
			}
		}
		if time.Now().After(deadline) {
			c.Fatalf("no leader for %s/%d: %v", topic, partition, err)
		}
		_ = p.clt.RefreshMetadata(topic)
		time.Sleep(50 * time.Millisecond)
	}
}

func (p *txnProducer) send(c *C, topic string, partition int32, value string) {
	key := fmt.Sprintf("%s-%d", topic, partition)
	seqKey := fmt.Sprintf("%s:%d", topic, partition)
	seq := p.seq[seqKey]
	now := time.Now()
	batch := &sarama.RecordBatch{
		// Producers must send 0 as the batch base offset; the broker
		// assigns the real one.
		FirstOffset:     0,
		Version:         2,
		ProducerID:      p.pid,
		ProducerEpoch:   p.epoch,
		FirstSequence:   seq,
		IsTransactional: true,
		FirstTimestamp:  now,
		MaxTimestamp:    now,
		LastOffsetDelta: 0,
		Records: []*sarama.Record{{
			Key:   []byte(key),
			Value: []byte(value),
		}},
	}
	req := &sarama.ProduceRequest{
		TransactionalID: &p.txnID,
		RequiredAcks:    sarama.WaitForAll,
		Timeout:         10000,
		Version:         3,
	}
	req.AddBatch(topic, partition, batch)

	resp, err := p.leader(c, topic, partition).Produce(req)
	c.Assert(err, IsNil)
	blk := resp.GetBlock(topic, partition)
	c.Assert(blk, NotNil)
	c.Assert(blk.Err, Equals, sarama.ErrNoError,
		Commentf("topic=%s partition=%d value=%s", topic, partition, value))
	p.seq[seqKey] = seq + 1
}

func (p *txnProducer) endTxn(c *C, commit bool) {
	resp, err := p.coord.EndTxn(&sarama.EndTxnRequest{
		TransactionalID:   p.txnID,
		ProducerID:        p.pid,
		ProducerEpoch:     p.epoch,
		TransactionResult: commit,
	})
	c.Assert(err, IsNil)
	c.Assert(resp.Err, Equals, sarama.ErrNoError)
}

func (p *txnProducer) close() {
	_ = p.coord.Close()
	_ = p.clt.Close()
}
