package consumer

import (
	"github.com/google/uuid"
	"github.com/tryfix/kstream/data"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
)

type IsolationLevel int8

const (
	ReadUncommitted IsolationLevel = iota
	ReadCommitted
)

type Config struct {
	*PartitionConsumerConfig
	GroupId string
}

func NewConfig() *Config {
	return &Config{
		PartitionConsumerConfig: NewPartitionConsumerConfig(),
	}
}

func NewPartitionConsumerConfig() *PartitionConsumerConfig {
	return &PartitionConsumerConfig{
		Logger:          log.NewNoopLogger(),
		MetricsReporter: metrics.NoopReporter(),
		RecordUuidExtractFunc: func(message *data.Record) uuid.UUID {
			return uuid.New()
		},
	}
}

func NewOffsetManagerConfig() *OffsetManagerConfig {
	return &OffsetManagerConfig{
		Logger:          log.NewNoopLogger(),
		MetricsReporter: metrics.NoopReporter(),
	}
}

type PartitionConsumerConfig struct {
	Id               string
	BootstrapServers []string
	IsolationLevel   IsolationLevel

	Logger                log.Logger
	MetricsReporter       metrics.Reporter
	RecordUuidExtractFunc RecordUuidExtractFunc
}

type OffsetManagerConfig struct {
	Id               string
	BootstrapServers []string

	Logger          log.Logger
	MetricsReporter metrics.Reporter
}

type OffsetManager interface {
	OffsetValid(topic string, partition int32, offset int64) (isValid bool, err error)
	GetOffsetLatest(topic string, partition int32) (offset int64, err error)
	GetOffsetOldest(topic string, partition int32) (offset int64, err error)
	Close() error
}

type GroupConsumerBuilder func(func(config *Config)) (Consumer, error)

type PartitionConsumerBuilder func(func(config *PartitionConsumerConfig)) (PartitionConsumer, error)

type OffsetManagerBuilder func(func(config *OffsetManagerConfig)) (OffsetManager, error)

//type GroupConsumerAdaptor func(conf Config) GroupConsumerBuilder

//type PartitionConsumerAdaptor func(conf PartitionConsumerConfig) PartitionConsumerBuilder

//type OffsetManagerAdaptor func(conf OffsetManagerConfig) OffsetManagerBuilder
