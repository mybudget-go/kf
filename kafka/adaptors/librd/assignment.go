package librd

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	kafka2 "github.com/gmbyapa/kstream/kafka"
)

type assignment struct {
	tpMap map[kafka2.TopicPartition]kafka2.Offset
	tps   []kafka.TopicPartition
}

func NewAssignment(tps []kafka.TopicPartition) *assignment {
	asgn := &assignment{
		tpMap: map[kafka2.TopicPartition]kafka2.Offset{},
		tps:   tps,
	}

	return asgn
}

func (a *assignment) claims() kafka2.Assignment {
	var consumerTps []kafka2.TopicPartition
	for _, tp := range a.tps {
		consumerTps = append(consumerTps, kafka2.TopicPartition{
			Topic:     *tp.Topic,
			Partition: tp.Partition,
		})
	}

	return consumerTps
}

func (a *assignment) toLibrd() []kafka.TopicPartition {
	return a.tps
}
