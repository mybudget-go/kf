package consumer

import (
	"context"
	"github.com/tryfix/kstream/data"
)

type GroupSession interface {
	Claims() []TopicPartition
	MarkOffset(tp TopicPartition, offset int64, metadata string)
	CommitOffset(tp TopicPartition, offset int64, metadata string)
}

type PartitionClaim interface {
	Topic() string
	Partition() int32
	Messages() <-chan *data.Record
}

type GroupHandler interface {
	OnPartitionRevoked(ctx context.Context, session GroupSession) error
	OnPartitionAssigned(ctx context.Context, session GroupSession) error
	Consume(ctx context.Context, session GroupSession, tp PartitionClaim) error
}
