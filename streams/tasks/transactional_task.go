package tasks

import (
	"context"
	"fmt"
	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/streams/topology"
	"github.com/tryfix/log"
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
	return t.processBatch(nil, records)
}
func (t *transactionalTask) processBatch(previousErr error, records []*Record) error {
	// Purge the store cache before the processing starts.
	// This will clear out any half processed states from state store caches.
	// The batch will either process and committed or will fail as a whole.
	for _, store := range t.subTopology.StateStores() {
		store.Purge()
	}

	// Check if producer needs a restart
	if fatalErr, ok := previousErr.(kafka.ProducerErr); ok {
		t.logger.Warn(fmt.Sprintf(`Fatal error %s while processing record batch`, fatalErr))
		if fatalErr.RequiresRestart() {
			t.logger.Warn(fmt.Sprintf(`Restarting producer due to %s`, fatalErr))
			if err := t.producer.Restart(); err != nil {
				log.Fatal(err) // TODO handle error
			}

			// Re init transaction
			t.logger.Warn(fmt.Sprintf(`Re Initing producer transaction due to %s`, fatalErr))
			if err := t.producer.InitTransactions(context.Background()); err != nil { // TODO handle the transaction timeout (context.Background())
				log.Fatal(err) // TODO handle error
			}
		}
	}

	if err := t.producer.BeginTransaction(); err != nil {
		t.logger.Warn(fmt.Sprintf(`BeginTransaction failed due to %s, retrying batch...`, err))
		return t.processBatch(err, records)
	}

	offsetMap := map[string]kafka.ConsumerOffset{}

	for _, record := range records {
		offsetMap[fmt.Sprintf(`%s-%d`, record.Topic(), record.Partition())] = kafka.ConsumerOffset{
			Topic:     record.Topic(),
			Partition: record.Partition(),
			Offset:    record.Offset() + 1,
			Meta:      record.String(), // TODO remove this
		}

		// This an already ignored record.(due to a process error)
		if record.ignore {
			continue
		}

		if err := t.process(record); err != nil {
			t.logger.WarnContext(record.Ctx(), fmt.Sprintf(`Batch process failed due to %s, retrying...`, err))
			return t.processBatch(err, records)
		}
	}

	meta, err := t.session.GroupMeta()
	if err != nil {
		if err := t.producer.AbortTransaction(context.Background()); err != nil {
			t.logger.Error(`transaction GroupMeta fetch failed`)
		}

		// Retrying the batch
		return t.processBatch(err, records)
	}

	var offsets []kafka.ConsumerOffset
	for _, offset := range offsetMap {
		offsets = append(offsets, offset)
	}

	t.logger.Trace(fmt.Sprintf(`Offset map: %+v, slice: %+v`, offsetMap, offsets))

	if err := t.producer.SendOffsetsToTransaction(context.Background(), offsets, meta); err != nil {
		if err := t.producer.AbortTransaction(context.Background()); err != nil {
			t.logger.Error(`transaction SendOffsetsToTransaction failed`)
		}

		// Retrying the batch
		return t.processBatch(err, records)
	}

	if err := t.producer.CommitTransaction(context.Background()); err != nil {
		t.logger.Warn(fmt.Sprintf(`Batch commit failed due to %s, aborting the transaction`, err))
		if err := t.producer.AbortTransaction(context.Background()); err != nil {
			t.logger.Error(`Abort transaction failed`)
		}

		// Retrying the batch
		t.logger.Warn(fmt.Sprintf(`Transaction aborted due to %s. Retrying...`, err))
		return t.processBatch(err, records)
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
