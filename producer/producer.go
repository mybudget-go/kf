/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package producer

import (
	"context"
	"github.com/tryfix/kstream/data"
)

type Builder func(conf func(*Config)) (Producer, error)

type RequiredAcks int

const (
	// NoResponse doesn't send any response, the TCP ACK is all you get.
	NoResponse RequiredAcks = 0

	// WaitForLeader waits for only the local commit to succeed before responding.
	WaitForLeader RequiredAcks = 1

	// WaitForAll waits for all in-sync replicas to commit before responding.
	// The minimum number of in-sync replicas is configured on the broker via
	// the `min.insync.replicas` configuration key.
	WaitForAll RequiredAcks = -1
)

func (ack RequiredAcks) String() string {
	a := `NoResponse`

	if ack == WaitForLeader {
		a = `WaitForLeader`
	}

	if ack == WaitForAll {
		a = `WaitForAll`
	}

	return a
}

type ConsumerOffset struct {
	Topic     string
	Partition int32
	Offset    int64
}

type Producer interface {
	Produce(ctx context.Context, message *data.Record) (partition int32, offset int64, err error)
	ProduceBatch(ctx context.Context, messages []*data.Record) error
	Close() error
}

type TransactionalProducer interface {
	Producer
	InitTransactions(ctx context.Context) error
	BeginTransaction() error
	SendOffsetsToTransaction(ctx context.Context, offsets []ConsumerOffset, meta string) error
	CommitTransaction(ctx context.Context) error
	AbortTransaction(ctx context.Context) error
}
