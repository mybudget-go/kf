package kafka

import (
	"context"
)

type Assignment []TopicPartition

// Len is part of sort.Interface.
func (list *Assignment) Len() int {
	return len(*list)
}

// Swap is part of sort.Interface.
func (list *Assignment) Swap(i, j int) {
	(*list)[i], (*list)[j] = (*list)[j], (*list)[i]
}

// Less is part of sort.Interface.
func (list *Assignment) Less(i, j int) bool {
	return i < j
}

type GroupSession interface {
	Assignment() Assignment
	GroupMeta() (*GroupMeta, error)
	TopicMeta() (TopicMeta, error)
	MarkOffset(ctx context.Context, record Record, meta string) error
	CommitOffset(ctx context.Context, record Record, meta string) error
}

type PartitionClaim interface {
	TopicPartition() TopicPartition
	Records() <-chan Record
}

type RebalanceHandler interface {
	OnPartitionRevoked(ctx context.Context, session GroupSession) error
	OnPartitionAssigned(ctx context.Context, session GroupSession) error
	OnLost() error
	Consume(ctx context.Context, session GroupSession, partition PartitionClaim) error
}
