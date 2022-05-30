package tasks

import (
	"fmt"
	"github.com/gmbyapa/kstream/pkg/async"
	"github.com/tryfix/metrics"
	"sync"
	"time"

	"github.com/tryfix/log"
)

type OnFlush func(records []*Record) error

type Buffer interface {
	Add(record *Record) error
	Flush() error
	Close() error
}

type BufferConfig struct {
	// Size defines the min num of records before the flush
	// starts(This includes messages in the state store changelogs).
	// Please note that this value has to be lesser than the
	// producer queue.buffering.max.messages
	Size int
	// FlushInterval defines minimum wait time before the flush starts
	FlushInterval time.Duration
}

type buffer struct {
	onFlush OnFlush
	size    int
	records []*Record

	tick              *time.Ticker
	mu                sync.Mutex
	stopping, stopped chan struct{}

	metrics struct {
		batchSize metrics.Counter
	}

	logger log.Logger
}

func newBuffer(config BufferConfig, onFlush OnFlush, logger log.Logger, reporter metrics.Reporter) *buffer {
	buf := &buffer{
		size:     config.Size,
		mu:       sync.Mutex{},
		tick:     time.NewTicker(config.FlushInterval),
		stopping: make(chan struct{}, 1),
		stopped:  make(chan struct{}, 1),
		onFlush:  onFlush,
		logger:   logger,
	}

	buf.metrics.batchSize = reporter.Counter(metrics.MetricConf{
		Path: `batch_size`,
	})

	go buf.run()

	return buf
}

func (b *buffer) Add(record *Record) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.records = append(b.records, record)

	if len(b.records) == b.size {
		return b.flush()
	}

	return nil
}

func (b *buffer) Flush() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.flush()
}

func (b *buffer) flush() error {
	count := len(b.records)
	if count < 1 { // Nothing to flush
		return nil
	}

	defer func() {
		b.logger.Info(fmt.Sprintf(`Buffer flushed with %d records`, count))
		b.records = nil

		b.metrics.batchSize.Count(float64(count), nil)
	}()

	return b.onFlush(b.records)
}

func (b *buffer) Close() error {
	b.logger.Info(`Buffer closing...`)
	defer b.logger.Info(`Buffer closed`)
	b.stopping <- struct{}{}
	<-b.stopped

	b.tick.Stop()

	// Do a one last flush before closing
	return b.Flush()
}

func (b *buffer) run() {
	defer async.LogPanicTrace(b.logger)
	defer b.logger.Info(`Buffer stopped`)

LP:
	for {
		select {
		case <-b.tick.C:
			if err := b.Flush(); err != nil {
				b.logger.Error(err)
			}
		case <-b.stopping:
			break LP
		}
	}

	b.stopped <- struct{}{}
}
