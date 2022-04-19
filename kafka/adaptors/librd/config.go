package librd

import (
	librdKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gmbyapa/kstream/kafka"
)

type GroupConsumerConfig struct {
	*kafka.GroupConsumerConfig
	Librd *librdKafka.ConfigMap
}

func (conf *GroupConsumerConfig) copy() *GroupConsumerConfig {
	librdCopy := librdKafka.ConfigMap{}
	for key, val := range *conf.Librd {
		librdCopy[key] = val
	}

	return &GroupConsumerConfig{
		GroupConsumerConfig: conf.GroupConsumerConfig.Copy(),
		Librd:               &librdCopy,
	}
}

type ConsumerConfig struct {
	*kafka.ConsumerConfig
	Librd *librdKafka.ConfigMap
}

func (conf *ConsumerConfig) copy() *ConsumerConfig {
	librdCopy := librdKafka.ConfigMap{}
	for key, val := range *conf.Librd {
		librdCopy[key] = val
	}

	return &ConsumerConfig{
		ConsumerConfig: conf.ConsumerConfig.Copy(),
		Librd:          &librdCopy,
	}
}

func NewGroupConsumerConfig() *GroupConsumerConfig {
	return &GroupConsumerConfig{
		Librd:               defaultLibrdGroupConfig(),
		GroupConsumerConfig: kafka.NewConfig(),
	}
}

func NewConsumerConfig() *ConsumerConfig {
	return &ConsumerConfig{
		Librd:          defaultLibrdConfig(),
		ConsumerConfig: kafka.NewPartitionConsumerConfig(),
	}
}

func defaultLibrdConfig() *librdKafka.ConfigMap {
	return &librdKafka.ConfigMap{
		"enable.partition.eof":   true,
		"go.logs.channel.enable": true,
		"log_level":              7,
		"auto.offset.reset":      "earliest",
	}
}

func defaultLibrdGroupConfig() *librdKafka.ConfigMap {
	return &librdKafka.ConfigMap{
		"session.timeout.ms":            6000,
		"partition.assignment.strategy": "cooperative-sticky",
		"go.logs.channel.enable":        true,
		"log_level":                     7,
	}
}
