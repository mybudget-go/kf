package offsets

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/tryfix/kstream/consumer"
	"github.com/tryfix/log"
)

type Config struct {
	Sarama           *sarama.Config
	BootstrapServers []string
	*consumer.OffsetManagerConfig
}

func NewOffsetManagerConfig() *Config {
	return &Config{
		Sarama:              sarama.NewConfig(),
		OffsetManagerConfig: consumer.NewOffsetManagerConfig(),
	}
}

type manager struct {
	client sarama.Client
	logger log.Logger
}

func NewOffsetManagerAdaptor(configurer func(*Config)) consumer.OffsetManagerBuilder {
	adptConf := NewOffsetManagerConfig()
	configurer(adptConf)
	return func(configurer func(*consumer.OffsetManagerConfig)) (consumer.OffsetManager, error) {
		defaultConf := consumer.NewOffsetManagerConfig()
		configurer(defaultConf)
		adptConf.Sarama.ClientID = defaultConf.Id
		adptConf.BootstrapServers = defaultConf.BootstrapServers
		adptConf.OffsetManagerConfig = defaultConf

		return NewManager(adptConf)
	}
}

func NewManager(config *Config) (consumer.OffsetManager, error) {
	logger := config.Logger.NewLog(log.Prefixed(`offset-manager`))
	client, err := sarama.NewClient(config.BootstrapServers, config.Sarama)
	if err != nil {
		return nil, fmt.Errorf(`cannot initiate builder deu to [%+v]`, err)
	}

	return &manager{client: client, logger: logger}, nil
}

func (m *manager) OffsetValid(topic string, partition int32, offset int64) (isValid bool, err error) {
	isValid, err = m.validate(topic, partition, offset)
	return
}

func (m *manager) GetOffsetLatest(topic string, partition int32) (offset int64, err error) {
	partitionStart, err := m.client.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		return offset, fmt.Errorf(`cannot get latest offset for %s-%d due to %w`, topic, partition, err)
	}

	return partitionStart, nil
}

func (m *manager) GetOffsetOldest(topic string, partition int32) (offset int64, err error) {
	partitionStart, err := m.client.GetOffset(topic, partition, sarama.OffsetOldest)
	if err != nil {
		return offset, fmt.Errorf(`cannot get oldes offset for %s-%d due to %w`, topic, partition, err)
	}

	return partitionStart, nil
}

func (m *manager) Close() error {
	return m.client.Close()
}

func (m *manager) validate(topic string, partition int32, offset int64) (isValid bool, err error) {
	startOffset, err := m.GetOffsetLatest(topic, partition)
	if err != nil {
		return false, fmt.Errorf(`offset validate failed for %s-%d due to %w`, topic, partition, err)
	}

	endOffset, err := m.GetOffsetOldest(topic, partition)
	if err != nil {
		return false, fmt.Errorf(`offset validate failed for %s-%d due to %w`, topic, partition, err)
	}

	return offset >= startOffset && offset < endOffset, nil
}
