package stores

import (
	"github.com/gmbyapa/kstream/backend"
	"github.com/gmbyapa/kstream/backend/memory"
)

type StoreOptions struct {
	//changelog        state_stores.ChangeLogger
	backend          backend.Backend
	backendBuilder   backend.Builder
	versionExtractor RecordVersionExtractor
	versionWriter    RecordVersionWriter
	//expiry            time.Duration
	buffered          bool
	bufferSize        int
	compactionEnabled bool
}

type Option func(config *StoreOptions)

func (c *StoreOptions) applyDefault() {
	c.backendBuilder = memory.Builder(memory.NewConfig())
}

func (c *StoreOptions) apply(options ...Option) {
	for _, opt := range options {
		opt(c)
	}
}

func WithVersionExtractor(etc RecordVersionExtractor) Option {
	return func(config *StoreOptions) {
		config.versionExtractor = etc
	}
}

func WithVersionWriter(wr RecordVersionWriter) Option {
	return func(config *StoreOptions) {
		config.versionWriter = wr
	}
}

//func WithChangeLogger(changelog state_stores.ChangeLogger) Option {
//	return func(config *StoreOptions) {
//		config.changelog = changelog
//	}
//}

func Compacated() Option {
	return func(options *StoreOptions) {
		options.compactionEnabled = true
	}
}

//func WithExpiry(d time.Duration) Option {
//	return func(options *StoreOptions) {
//		options.expiry = d
//	}
//}

func Buffered(size int) Option {
	return func(options *StoreOptions) {
		options.buffered = true
		options.bufferSize = size
	}
}

func WithBackend(backend backend.Backend) Option {
	return func(config *StoreOptions) {
		config.backend = backend
	}
}

func WithBackendBuilder(builder backend.Builder) Option {
	return func(config *StoreOptions) {
		config.backendBuilder = builder
	}
}
