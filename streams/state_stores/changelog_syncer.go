package state_stores

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/pkg/errors"
	"github.com/gmbyapa/kstream/streams/stores"
	logger "github.com/tryfix/log"
)

type ChangelogStatus string

type changelogSyncer struct {
	tp          kafka.TopicPartition
	offsetStore OffsetStore
	consumer    kafka.PartitionConsumer
	logger      logger.Logger
	store       stores.Store
	stopping    chan struct{}
	running     chan struct{}

	mu *sync.Mutex
}

func (lg *changelogSyncer) Sync(ctx context.Context, synced chan struct{}) error {
	offset, err := lg.offsetStore.Committed(lg.tp)
	if err != nil {
		return errors.Wrap(err, `cannot get committed offset`)
	}

	lg.logger.Info(fmt.Sprintf(`Offset %s found for %s`, offset, lg.tp))

	partition, err := lg.consumer.ConsumePartition(ctx, lg.tp.Topic, lg.tp.Partition, offset)
	if err != nil {
		return errors.Wrapf(err, `partition consume failed on %s`, lg.tp)
	}

	return lg.startSync(ctx, partition, synced)
}

func (lg *changelogSyncer) startSync(ctx context.Context, partition kafka.Partition, synced chan struct{}) error {
	ticker := time.NewTicker(1 * time.Second)
	endOnce := sync.Once{}

	lg.logger.Info(`Syncing...`)

	defer endOnce.Do(func() {
		ticker.Stop()
	})

	defer func() {
		if err := partition.Close(); err != nil {
			lg.logger.Warn(`Partition consumer close failed`)
		} else {
			lg.logger.Info(`Consumer loop closed`)
		}

		close(lg.running)
		close(synced)
	}()

	var syncedCount int64
	go func(tic *time.Ticker) {
		for range tic.C {
			if partition.EndOffset() < 1 {
				continue
			}
			lg.logger.Info(
				fmt.Sprintf(
					`Sync progress - [%d]%% done (%d/%d)`,
					atomic.LoadInt64(&syncedCount)*100/int64(partition.EndOffset()),
					atomic.LoadInt64(&syncedCount), int64(partition.EndOffset())))
		}
	}(ticker)
	syncStarted := time.Now()

MAIN:
	for {
		select {
		case <-ctx.Done():
			lg.logger.Info(`Consumer loop stopping due to context cancel`)
			break MAIN
		case <-lg.stopping:
			lg.logger.Info(`Consumer loop stopping due to stop signal`)
			break MAIN
		case event := <-partition.Events():
			switch e := event.(type) {
			case kafka.Record:
				atomic.AddInt64(&syncedCount, 1)

				// Handle tombstones
				if len(e.Value()) < 1 {
					if err := lg.store.Backend().Delete(e.Key()); err != nil {
						return errors.Wrapf(err, `Cannot delete(tombstone) message due to %s`, err)
					}
				} else {
					// For indexed records we cannot directly update the backend
					if _, ok := lg.store.(stores.IndexedStore); ok {
						panic(`KStream does not support indexed stores`)
					} else {
						if err := lg.store.Backend().Set(e.Key(), e.Value(), 0); err != nil {
							return errors.Wrapf(err, `Cannot write message due to %s`, err)
						}
					}
				}

				// Non-persistent backends cannot store the offset
				if !lg.store.Backend().Persistent() {
					continue
				}

				// TODO offset has to be offset+1
				if err := lg.offsetStore.Commit(ctx, lg.tp, kafka.Offset(e.Offset())); err != nil {
					lg.logger.Warn(fmt.Sprintf(`Cannot commit offset due to %s`, err))
					continue
				}

			case *kafka.PartitionEnd:
				endOnce.Do(func() {
					lg.logger.Info(fmt.Sprintf(
						`Partition read ended. Restored %d records in %s`,
						syncedCount,
						time.Since(syncStarted).String()))
					ticker.Stop()
					synced <- struct{}{}
				})

			case *kafka.Error:
				lg.logger.Error(e)
			}
		}
	}

	return nil
}

func (lg *changelogSyncer) Stop() error {
	lg.logger.Info(`Syncer stopping...`)
	defer lg.logger.Info(`Syncer stopped`)

	close(lg.stopping)
	<-lg.running

	return nil
}
