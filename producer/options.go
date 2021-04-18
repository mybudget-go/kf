package producer

import (
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
)

type Options struct {
	Logger          log.Logger
	MetricsReporter metrics.Reporter
}

type Option func(*Options)

func (opts *Options) ApplyDefault() {
	opts.Logger = log.NewNoopLogger()
	opts.MetricsReporter = metrics.NoopReporter()
}

func (opts *Options) Apply(options ...Option) {
	for _, option := range options {
		option(opts)
	}
}

func WithLogger(logger log.Logger) Option {
	return func(options *Options) {
		options.Logger = logger
	}
}

func WithMetricsReporter(reporter metrics.Reporter) Option {
	return func(options *Options) {
		options.MetricsReporter = reporter
	}
}
