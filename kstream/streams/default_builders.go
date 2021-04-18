package streams

import (
	saramaBase "github.com/Shopify/sarama"
	"github.com/tryfix/kstream/admin"
	"github.com/tryfix/kstream/backend"
	"github.com/tryfix/kstream/backend/memory"
	"github.com/tryfix/kstream/consumer"
	"github.com/tryfix/kstream/consumer/adaptors/sarama"
	"github.com/tryfix/kstream/consumer/adaptors/sarama/offsets"
	"github.com/tryfix/kstream/kstream/serdes"
	"github.com/tryfix/kstream/kstream/store"
	"github.com/tryfix/kstream/producer"
	"github.com/tryfix/kstream/producer/adaptors/librd"
)

type DefaultBuilders struct {
	Producer          producer.Builder
	GroupConsumer     consumer.GroupConsumerBuilder
	PartitionConsumer consumer.PartitionConsumerBuilder
	Store             store.Builder
	IndexedStore      store.IndexedStoreBuilder
	Backend           backend.Builder
	StateStore        store.StateStoreBuilder
	OffsetManager     consumer.OffsetManagerBuilder
	KafkaAdmin        admin.KafkaAdmin
}

func (dbs *DefaultBuilders) build(configs *Config) {
	// apply options

	// default backend builder will be memory
	backendBuilderConfig := memory.NewConfig()
	backendBuilderConfig.Logger = configs.Logger
	backendBuilderConfig.MetricsReporter = configs.MetricsReporter
	dbs.Backend = memory.Builder(backendBuilderConfig)

	dbs.Backend = memory.Builder(memory.NewConfig())

	dbs.Store = func(name string, keyEncoder, valEncoder serdes.SerDes, options ...store.Options) (store.Store, error) {
		return store.NewStore(name, keyEncoder, valEncoder, append(
			options,
			store.WithBackendBuilder(dbs.Backend),
			store.WithLogger(configs.Logger),
		)...)
	}

	dbs.IndexedStore = func(name string, keyEncoder, valEncoder serdes.SerDes, indexes []store.Index, options ...store.Options) (store.IndexedStore, error) {
		return store.NewIndexedStore(name, keyEncoder, valEncoder, indexes, append(
			options,
			store.WithBackendBuilder(dbs.Backend),
			store.WithLogger(configs.Logger),
		)...)
	}

	dbs.Producer = librd.NewProducerBuilder(func(config *librd.Config) {
		config.BootstrapServers = configs.BootstrapServers
		config.Transactional.Enabled = configs.Producer.Transactional
		config.Acks = configs.Producer.RequiredAcks
		config.Idempotent = configs.Producer.Idempotent

		config.Logger = configs.Logger
		config.MetricsReporter = configs.MetricsReporter
	})

	dbs.GroupConsumer = sarama.NewGroupConsumerAdaptor(func(config *sarama.Config) {
		config.Sarama.Version = saramaBase.V2_4_0_0
		config.BootstrapServers = configs.BootstrapServers
		config.Id = configs.ApplicationId
		config.GroupId = configs.ApplicationId
		config.IsolationLevel = configs.Consumer.IsolationLevel

		config.Logger = configs.Logger
		config.MetricsReporter = configs.MetricsReporter
	})

	dbs.PartitionConsumer = sarama.NewPartitionConsumerBuilder(func(config *sarama.PartitionConsumerConfig) {
		config.Sarama.Version = saramaBase.V2_4_0_0
		config.Id = configs.ApplicationId
		config.BootstrapServers = configs.BootstrapServers

		config.Logger = configs.Logger
		config.MetricsReporter = configs.MetricsReporter
	})

	dbs.OffsetManager = offsets.NewOffsetManagerAdaptor(func(config *offsets.Config) {
		config.Sarama.Version = saramaBase.V2_4_0_0
		config.Id = configs.ApplicationId
		config.BootstrapServers = configs.BootstrapServers

		config.Logger = configs.Logger
		config.MetricsReporter = configs.MetricsReporter
	})

	dbs.KafkaAdmin = admin.NewKafkaAdmin(configs.BootstrapServers,
		admin.WithLogger(configs.Logger),
	)
}
