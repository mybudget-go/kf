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
	Store        stores.Builder
	IndexedStore stores.IndexedStoreBuilder
	Backend      backend.Builder
	KafkaAdmin   kafka.Admin
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
		dbs.KafkaAdmin = saramaAdpt.NewAdmin(configs.BootstrapServers,
			saramaAdpt.WithLogger(configs.Logger),
		)
	}
}
