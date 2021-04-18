package sarama

import (
	"github.com/Shopify/sarama"
	"github.com/tryfix/kstream/consumer"
)

type Config struct {
	*consumer.Config
	Sarama *sarama.Config
}

type PartitionConsumerConfig struct {
	*consumer.PartitionConsumerConfig
	Sarama *sarama.Config
}

func NewGroupConsumerConfig() *Config {
	return &Config{
		Config: consumer.NewConfig(),
		Sarama: sarama.NewConfig(),
	}
}

func NewPartitionConsumerConfig() *PartitionConsumerConfig {
	return &PartitionConsumerConfig{
		Sarama:                  sarama.NewConfig(),
		PartitionConsumerConfig: consumer.NewPartitionConsumerConfig(),
	}
}
