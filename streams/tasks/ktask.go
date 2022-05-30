package tasks

import (
	"context"
	"fmt"
	"github.com/gmbyapa/kstream/pkg/errors"
	"sync"
	"time"

	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/pkg/async"
	"github.com/gmbyapa/kstream/streams/topology"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
)

type FailedMessageHandler func(err error, record kafka.Record)

type taskOptions struct {
	buffer               BufferConfig
	failedMessageHandler FailedMessageHandler
}

func (tOpts *taskOptions) setDefault() {
	// Set default opts
	tOpts.buffer.FlushInterval = 1 * time.Second
	tOpts.buffer.Size = 1000
	tOpts.failedMessageHandler = func(err error, record kafka.Record) {}
}

func (tOpts *taskOptions) apply(opts ...TaskOpt) {
	for _, opt := range opts {
		opt(tOpts)
	}
}

type TaskOpt func(*taskOptions)

func WithBufferFlushInterval(interval time.Duration) TaskOpt {
	return func(options *taskOptions) {
		options.buffer.FlushInterval = interval
	}
}

func WithBufferSize(size int) TaskOpt {
	return func(options *taskOptions) {
		options.buffer.Size = size
	}
}

func WithFailedMessageHandler(handler FailedMessageHandler) TaskOpt {
	return func(options *taskOptions) {
		options.failedMessageHandler = handler
	}
}

type TaskID interface {
	String() string
	// UniqueID is uniques withing the task manager and
	UniqueID() string
	Partition() int32
}

type Task interface {
	ID() TaskID
	Init(ctx topology.SubTopologyContext) error
	Sync() error
	Ready() error
	Start(ctx context.Context, claim kafka.PartitionClaim, groupSession kafka.GroupSession)
	Store(name string) topology.StateStore
	Stop() error
}

type taskId struct {
	id        int
	hash      string
	prefix    string
	partition int32
}

func (t taskId) UniqueID() string {
	return t.hash
}

func (t taskId) String() string {
	return fmt.Sprintf(`%s#%d`, t.prefix, t.id)
}

func (t taskId) Partition() int32 {
	return t.partition
}

type task struct {
	id           TaskID
	subTopology  topology.SubTopology
	global       bool
	session      kafka.GroupSession
	blockingMode bool
	logger       log.Logger
	stopping     chan struct{}

	buffer Buffer

	options *taskOptions

	ctx topology.SubTopologyContext

	consumerLoops sync.WaitGroup

	producer kafka.Producer
	metrics  struct {
		reporter                        metrics.Reporter
		processLatencyMicroseconds      metrics.Observer
		batchProcessLatencyMicroseconds metrics.Observer
	}

	shutDownOnce sync.Once

	runGroup *async.RunGroup
}

func (t *task) ID() TaskID {
	return t.id
}

func (t *task) Init(ctx topology.SubTopologyContext) error {
	t.metrics.processLatencyMicroseconds = t.metrics.reporter.Observer(metrics.MetricConf{
		Path:   `process_latency_microseconds`,
		Labels: []string{`topic_partition`},
	})

	t.metrics.batchProcessLatencyMicroseconds = t.metrics.reporter.Observer(metrics.MetricConf{
		Path: `batch_process_latency_microseconds`,
	})

	defer func() {
		// Each StateStore instance in the task has to restored before the processing stats
		for _, store := range t.subTopology.StateStores() {
			stateStore := store
			t.runGroup.Add(func(opts *async.Opts) error {
				stateSynced := make(chan struct{}, 1)
				go func() {
					defer async.LogPanicTrace(t.logger)

					select {
					// Once the state is synced we can close the ChangelogSyncer
					case <-stateSynced:
						if err := stateStore.Stop(); err != nil {
							panic(err.Error())
						}
					case <-opts.Stopping():
						if err := stateStore.Stop(); err != nil {
							panic(err.Error())
						}
					}
				}()

				return stateStore.Sync(ctx, stateSynced)
			})
		}
	}()

	return t.subTopology.Init(ctx)
}

func (t *task) Sync() error {
	return t.runGroup.Run()
}

func (t *task) Ready() error {
	if err := t.runGroup.Ready(); err != nil {
		return err
	}

	t.logger.Info(`State Recovered`)
	return nil
}

func (t *task) process(record *Record) error {
	defer func(since time.Time) {
		t.metrics.processLatencyMicroseconds.Observe(float64(time.Since(since).Microseconds()), map[string]string{
			`topic_partition`: fmt.Sprintf(`%s-%d`, record.Topic(), record.Partition()),
		})
	}(time.Now())

	_, _, _, err := t.subTopology.Source(record.Topic()).
		Run(topology.NewRecordContext(record), record.Key(), record.Value())
	if err != nil {
		// If this is a kafka producer error return it(will be retried), otherwise ignore and exclude it form
		// re-processing(only the kafka errors can be retried here)
		assert := func(err error) bool {
			_, ok := err.(kafka.ProducerErr)
			return ok
		}
		if producerErr := errors.UnWrapRecursivelyUntil(err, assert); producerErr != nil {
			return producerErr
		}

		// Send record to DLQ handler and mark record as ignored,
		// so it will be excluded from next batch
		t.options.failedMessageHandler(err, record)
		record.ignore = true

		return err
	}

	return nil
}

func (t *task) Start(ctx context.Context, claim kafka.PartitionClaim, _ kafka.GroupSession) {
	t.blockingMode = true

	stopping := make(chan struct{}, 1)
	t.consumerLoops.Add(1)

	go func() {
		<-t.stopping
		t.logger.Info(fmt.Sprintf(`Stop signal received. Stopping message loop %s`, claim.TopicPartition()))
		stopping <- struct{}{}
	}()

	var once sync.Once

MAIN:
	for {
		select {
		case <-stopping:
			break MAIN
		case record, ok := <-claim.Records():
			if !ok {
				// channel is closed
				break MAIN
			}

			once.Do(func() {
				t.logger.Info(fmt.Sprintf(`Starting offset %s`, record))
			})
			if err := t.buffer.Add(NewTaskRecord(record)); err != nil {
				t.options.failedMessageHandler(err, record)
			}
		}
	}

	t.consumerLoops.Done()
	t.logger.Info(fmt.Sprintf(`Message loop stopped for %s`, claim.TopicPartition()))
}

func (t *task) Stop() error {
	t.shutdown(nil)
	return nil
}

func (t *task) shutdown(err error) {
	t.shutDownOnce.Do(func() {
		if err != nil {
			t.logger.Info(fmt.Sprintf(`Stopping..., due to %s`, err))
		} else {
			t.logger.Info(`Stopping...`)
		}
		defer t.logger.Info(`Stopped`)

		t.runGroup.Stop()

		if t.blockingMode {
			t.logger.Info(`Waiting until processing stopped...`)
			close(t.stopping)
			t.consumerLoops.Wait()
			t.logger.Info(`Processing stopped`)
		}

		if err := t.buffer.Close(); err != nil {
			t.logger.Warn(err)
		}

		// TODO complete this
		// if err := t.subTopology.Destroy(); err != nil{}

		// Close all the state stores
		wg := &sync.WaitGroup{}
		for _, store := range t.subTopology.StateStores() {
			wg.Add(1)
			go func(wg *sync.WaitGroup, ins topology.StateStore) {
				defer wg.Done()

				if err := ins.Close(); err != nil {
					t.logger.Warn(fmt.Sprintf(`LoggableStateStore close error due to %s`, err))
				}
			}(wg, store)
		}
		wg.Wait()

		if t.producer != nil {
			if err := t.producer.Close(); err != nil {
				t.logger.Error(err)
			}
		}
	})
}

func (t *task) Store(name string) topology.StateStore {
	return t.subTopology.StateStores()[name]
}

func (t *task) onFlush(records []*Record) error {
	return t.processBatch(nil, records)
}

func (t *task) processBatch(err error, records []*Record) error {
	// The processing has to stop if the task status is shutting down

	// Purge the store cache before the processing starts. This will clear out any half processed states from state stores
	for _, store := range t.subTopology.StateStores() {
		store.Purge()
	}

	if fatalErr, ok := err.(kafka.ProducerErr); ok {
		t.logger.Warn(fmt.Sprintf(`Fatal error %s while processing record batch`, fatalErr))
		if fatalErr.RequiresRestart() {
			t.logger.Warn(fmt.Sprintf(`Restarting producer due to %s`, fatalErr))
			if err := t.producer.Restart(); err != nil {
				log.Fatal(err) // TODO handle error
			}
		}
	}

	for _, record := range records {
		// ignore messages from reprocessing if they marked as excluded
		if record.ignore {
			continue
		}

		if err := t.process(record); err != nil {
			// if this a process error, abort the batch and then exclude the record and put into a errorHandler
			// and then retry the batch without the failed message
			t.logger.WarnContext(record.Ctx(), fmt.Sprintf(`Batch process failed due to %s, retrying...`, err))
			return t.processBatch(err, records)
		}
	}

	// Flush(Commit) all the stores in the Task
	for _, store := range t.subTopology.StateStores() {
		if err := store.Flush(); err != nil {
			return err // TODO wrap error
		}
	}

	t.logger.Info(fmt.Sprintf(
		`Transaction committed(offset range[%d-%d])`,
		records[0].Offset(),
		records[len(records)-1].Offset()))

	return nil
}
