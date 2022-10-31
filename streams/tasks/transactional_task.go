package tasks

import (
	"context"
	"fmt"
	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/streams/topology"
	"github.com/tryfix/log"
	"strconv"
	"time"
)

type transactionalTask struct {
	*task
	producer kafka.TransactionalProducer
}

func (t *transactionalTask) Init(ctx topology.SubTopologyContext) error {
	if err := t.task.Init(ctx); err != nil {
		return err
	}

	return t.producer.InitTransactions(context.Background())
}

func (t *transactionalTask) onFlush(records []*Record) error {
	defer func(since time.Time) {
		t.metrics.batchFlushLatencyMicroseconds.Observe(float64(time.Since(since).Microseconds()), nil)
		t.metrics.batchSize.Count(float64(len(records)), nil)
	}(time.Now())

	return t.processBatch(nil, records, 0)
}

func (t *transactionalTask) processBatch(previousErr error, records []*Record, itr int) error {
	itr++

	defer func(since time.Time) {
		t.metrics.batchProcessLatencyMicroseconds.Observe(float64(time.Since(since).Microseconds()), map[string]string{
			`retry_count`: strconv.Itoa(itr),
		})
	}(time.Now())

	// Purge the store cache before the processing starts.
	// This will clear out any half processed states from state store caches.
	// The batch will either be processed and committed or will fail as a whole.
	for _, store := range t.subTopology.StateStores() {
		store.Purge()
	}

	// Check if producer needs a restart
	if producerErr, ok := previousErr.(kafka.ProducerErr); ok {
		t.logger.Warn(fmt.Sprintf(`Fatal error %s while processing record batch`, producerErr))
		if producerErr.RequiresRestart() {
			t.logger.Warn(fmt.Sprintf(`Restarting producer due to %s`, producerErr))
			if err := t.producer.Restart(); err != nil {
				log.Fatal(err) // TODO handle error
			}

			// Re init transaction
			t.logger.Warn(fmt.Sprintf(`Re Initing producer transaction due to %s`, producerErr))
			if err := t.producer.InitTransactions(context.Background()); err != nil { // TODO handle the transaction timeout (context.Background())
				log.Fatal(err) // TODO handle error
			}
		}
	}

	if err := t.producer.BeginTransaction(); err != nil {
		t.logger.Warn(fmt.Sprintf(`BeginTransaction failed due to %s, retrying batch...`, err))
		return t.processBatch(err, records, itr)
	}

	offsetMap := map[string]kafka.ConsumerOffset{}
	for _, record := range records {
		offsetMap[fmt.Sprintf(`%s-%d`, record.Topic(), record.Partition())] = kafka.ConsumerOffset{
			Topic:     record.Topic(),
			Partition: record.Partition(),
			Offset:    record.Offset() + 1,
		}

		// This an already ignored record.(due to a process error)
		if record.ignore {
			continue
		}

		if err := t.process(record); err != nil {
			t.logger.WarnContext(record.Ctx(), fmt.Sprintf(`Batch process failed due to %s, retrying...`, err))
			if txAbErr := t.producer.AbortTransaction(context.Background()); txAbErr != nil {
				t.logger.Error(fmt.Sprintf(`transaction abort failed due to %s`, txAbErr))
			}

			return t.processBatch(err, records, itr)
		}
	}

	meta, err := t.session.GroupMeta()
	if err != nil {
		t.logger.Error(fmt.Sprintf(`transaction Consumer GroupMeta fetch failed due to %s, abotring transactions`, err))
		if txAbErr := t.producer.AbortTransaction(context.Background()); txAbErr != nil {
			t.logger.Error(fmt.Sprintf(`transaction abort failed due to %s`, txAbErr))
		}

		// Retrying the batch
		return t.processBatch(err, records, itr)
	}

	var offsets []kafka.ConsumerOffset
	for _, offset := range offsetMap {
		offsets = append(offsets, offset)
	}

	if err := t.producer.SendOffsetsToTransaction(context.Background(), offsets, meta); err != nil {
		// Retrying the batch
		return t.processBatch(err, records, itr)
	}

	if err := t.producer.CommitTransaction(context.Background()); err != nil {
		// Retrying the batch
		t.logger.Warn(fmt.Sprintf(`Transaction commit failed due to %s. Retrying...`, err))
		return t.processBatch(err, records, itr)
	}

	// Flush(Commit) all the stores in the Task
	for _, store := range t.subTopology.StateStores() {
		if err := store.Flush(); err != nil {
			return err // TODO wrap error
		}
	}

	t.logger.Info(fmt.Sprintf(`Transaction committed(offsets %+v)`, offsets))

	return nil
}
