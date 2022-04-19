package state_stores

import (
	"context"
	"time"

	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/streams/topology"
)

type changeLogger struct {
	producer kafka.Producer
	tp       kafka.TopicPartition
}

type changeLoggerBuilder struct {
	topicFormatter ChangelogTopicFormatter
}

type ChangelogLoggerBuilder interface {
	Build(ctx topology.SubTopologyContext, store string) (topology.ChangeLogger, error)
}

func NewChangeLogger(producer kafka.Producer, tp kafka.TopicPartition) topology.ChangeLogger {
	return &changeLogger{producer: producer, tp: tp}
}

func NewChangeLoggerBuilder(topic ChangelogTopicFormatter) ChangelogLoggerBuilder {
	return &changeLoggerBuilder{topicFormatter: topic}
}

func (b *changeLoggerBuilder) Build(ctx topology.SubTopologyContext, store string) (topology.ChangeLogger, error) {
	return NewChangeLogger(ctx.Producer(), kafka.TopicPartition{
		Topic:     b.topicFormatter(store)(ctx),
		Partition: ctx.Partition(),
	}), nil
}

func (c *changeLogger) Log(ctx context.Context, key, value []byte) error {
	record := c.producer.NewRecord(ctx, key, value, c.tp.Topic, c.tp.Partition, time.Now(), nil, ``)
	if txPrd, ok := c.producer.(kafka.TransactionalProducer); ok {
		return txPrd.ProduceAsync(ctx, record)
	}

	_, _, err := c.producer.ProduceSync(ctx, record)

	return err
}
