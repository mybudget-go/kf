package sarama

import (
	"github.com/Shopify/sarama"
	"github.com/tryfix/kstream/consumer"
)

type groupSession struct {
	tps     []consumer.TopicPartition
	session sarama.ConsumerGroupSession
}

func (g *groupSession) Claims() []consumer.TopicPartition {
	return g.tps
}

func (g *groupSession) MarkOffset(tp consumer.TopicPartition, offset int64, metadata string) {
	g.session.MarkOffset(tp.Topic, tp.Partition, offset, metadata)
}

func (g *groupSession) CommitOffset(tp consumer.TopicPartition, offset int64, metadata string) {
	g.session.MarkOffset(tp.Topic, tp.Partition, offset, metadata)
}
