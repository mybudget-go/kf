package librd

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/tryfix/kstream/producer"
)

type Config struct {
	Librd *kafka.ConfigMap

	*producer.Config
}

func NewConfig() *Config {
	return &Config{
		Librd:  &kafka.ConfigMap{},
		Config: producer.NewConfig(),
	}
}

func (c *Config) validate() error {
	return nil
}
