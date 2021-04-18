package streams

import (
	"context"
	"fmt"
	"github.com/tryfix/kstream/data"
	kContext "github.com/tryfix/kstream/kstream/context"
	"github.com/tryfix/kstream/producer"
)

type TaskId interface {
	String() string
}

type KTaskInfo struct {
	Topic     string
	Partition int32
}

func (t KTaskInfo) String() string {
	return fmt.Sprintf(`%s-%d`, t.Topic, t.Partition)
}

func newTaskId(topic string, partition int32) TaskId {
	return KTaskInfo{
		Topic:     topic,
		Partition: partition,
	}
}

type kTask struct {
	id            TaskId
	source        *KSource
	transactional struct {
		enabled  bool
		producer producer.TransactionalProducer
	}
}

func (t *kTask) Init(ctx context.Context) error {
	if t.transactional.enabled {
		return t.transactional.producer.InitTransactions(ctx)
	}

	return nil
}

func (t *kTask) Run(message *data.Record) error {
	// if topology has sinks start a transaction
	if t.transactional.enabled {
		if err := t.transactional.producer.BeginTransaction(); err != nil {
			return err
		}

		_, _, _, err := t.source.Run(kContext.FromRecord(context.Background(), message), message.Key, message.Value)
		if err != nil {
			if err := t.transactional.producer.AbortTransaction(context.Background()); err != nil {
				return err
			}
			return err
		}

		if err := t.transactional.producer.CommitTransaction(context.Background()); err != nil {
			return err
		}
	}

	_, _, _, err := t.source.Run(kContext.FromRecord(context.Background(), message), message.Key, message.Value)
	if err != nil {
		return err
	}

	return nil
}
