package consumer

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/tryfix/kstream/data"
)

type RecordUuidExtractFunc func(message *data.Record) uuid.UUID

type TopicPartition struct {
	Topic     string
	Partition int32
}

func (tp TopicPartition) String() string {
	return fmt.Sprintf(`%s-%d`, tp.Topic, tp.Partition)
}

type Consumer interface {
	Subscribe(ctx context.Context, tps []string, handler GroupHandler) error
	Errors() <-chan error
}

type PartitionConsumer interface {
	Subscribe(ctx context.Context, topic string, partition int32, offset Offset) (<-chan Event, error)
	Errors() <-chan error
}

type Offset int64

const (
	Earliest Offset = -2
	Latest   Offset = -1
)

func (o Offset) String() string {
	switch o {
	case -2:
		return `Earliest`
	case -1:
		return `Latest`
	default:
		return fmt.Sprint(int(o))
	}
}
