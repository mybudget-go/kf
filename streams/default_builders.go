package streams

import (
	"github.com/gmbyapa/kstream/backend"
	"github.com/gmbyapa/kstream/backend/memory"
	"github.com/gmbyapa/kstream/kafka"
	saramaAdpt "github.com/gmbyapa/kstream/kafka/adaptors/sarama"
	"github.com/gmbyapa/kstream/streams/encoding"
	"github.com/gmbyapa/kstream/streams/stores"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
)

type DefaultBuilders struct {
	//Producer kafka.ProducerBuilder
	//GroupConsumer     kafka.GroupConsumerProvider
	//PartitionConsumer kafka.ConsumerBuilder
	Store        stores.Builder
	IndexedStore stores.IndexedStoreBuilder
	Backend      backend.Builder
	//StateStore        stores.StoreBuilder
	//OffsetManager     kafka.OffsetManagerBuilder
	KafkaAdmin kafka.Admin
}

func (dbs *DefaultBuilders) setup(configs *Config) {
	// apply options
	configs.Logger = configs.Logger.NewLog(log.Prefixed(`kStream`))

	// default backend builder will be memory
	backendBuilderConfig := memory.NewConfig()
	backendBuilderConfig.MetricsReporter = configs.MetricsReporter.Reporter(metrics.ReporterConf{
		Subsystem: "kstream_backends",
	})
	dbs.Backend = memory.Builder(backendBuilderConfig)

	dbs.Store = func(name string, keyEncoder, valEncoder encoding.Encoder, options ...stores.Option) (stores.Store, error) {
		return stores.NewStore(name, keyEncoder, valEncoder, append(
			options,
			stores.WithBackendBuilder(dbs.Backend),
		)...)
	}

	dbs.IndexedStore = func(name string, keyEncoder, valEncoder encoding.Encoder, indexes []stores.Index, options ...stores.Option) (stores.IndexedStore, error) {
		return stores.NewIndexedStore(name, keyEncoder, valEncoder, indexes, append(
			options,
			stores.WithBackendBuilder(dbs.Backend),
		)...)
	}

	// configure producer/ consumer adaptors
	dbs.configureAdaptors(`librd`, configs)
}

func (dbs *DefaultBuilders) configureAdaptors(typ string, configs *Config) {
	switch typ {
	case `librd`:
		//dbs.Producer = librd3Adpt.NewProducerAdaptor(func(config *librd3Adpt.ProducerConfig) {
		//	config.BootstrapServers = configs.BootstrapServers
		//	config.Transactional.Enabled = configs.Processing.Guarantee == ExactlyOnce
		//	config.Acks = configs.Producer.Acks
		//	config.Logger = configs.Logger
		//	config.MetricsReporter = configs.MetricsReporter
		//
		//	if err := config.Librd.SetKey(`go.events.channel.size`, 1000); err != nil {
		//		panic(err)
		//	}
		//
		//	if err := config.Librd.SetKey(`go.produce.channel.size`, 1000); err != nil {
		//		panic(err)
		//	}
		//})

		//var conf
		//configK := kafka.NewPartitionConsumerConfig()
		//configK.

		//config := librd3Adpt.NewConsumerConfig()
		//config.ConsumerConfig = configs.Consumer.ConsumerConfig
		//dbs.PartitionConsumer = librd3Adpt.NewPartitionConsumerAdaptor(config)

		//groupConfig := librd3Adpt.NewGroupConsumerConfig()
		//dbs.GroupConsumer = librd3Adpt.NewGroupConsumerAdaptor(groupConfig).NewBuilder(configs.Consumer)

		dbs.KafkaAdmin = saramaAdpt.NewAdmin(configs.BootstrapServers,
			saramaAdpt.WithLogger(configs.Logger),
		)

		//case `franz-go`:
		//	dbs.Producer = franzgoAdpt.NewProducerAdaptor(func() *franzgoAdpt.ProducerConfig {
		//		config := franzgoAdpt.NewProducerConfig()
		//		config.BootstrapServers = configs.BootstrapServers
		//		config.Transactional.Enabled = configs.Processing.Guarantee == ExactlyOnce
		//		config.Acks = configs.Producer.RequiredAcks
		//		config.Logger = configs.Logger
		//		config.MetricsReporter = configs.MetricsReporter
		//
		//		return config
		//	})
		//
		//	dbs.GroupConsumer = franzgoAdpt.NewGroupConsumerAdaptor(func(config *franzgoAdpt.ConsumerConfig) {
		//		config.BootstrapServers = configs.BootstrapServers
		//		config.Id = configs.ApplicationId
		//		config.GroupId = configs.ApplicationId
		//		config.IsolationLevel = configs.Consumer.IsolationLevel
		//
		//		if configs.Processing.Guarantee == ExactlyOnce {
		//			config.IsolationLevel = kafka.ReadCommitted
		//		}
		//
		//		config.Logger = configs.Logger
		//		config.MetricsReporter = configs.MetricsReporter
		//	})
		//
		//	dbs.PartitionConsumer = franzgoAdpt.NewPartitionConsumerAdaptor(func(config *franzgoAdpt.PartitionConsumerConfig) {
		//		config.Id = configs.ApplicationId
		//		config.BootstrapServers = configs.BootstrapServers
		//		config.Logger = configs.Logger
		//		config.MetricsReporter = configs.MetricsReporter
		//	})
		//
		//	dbs.KafkaAdmin = saramaAdpt.NewAdmin(configs.BootstrapServers,
		//		saramaAdpt.WithLogger(configs.Logger),
		//	)
	}
}
