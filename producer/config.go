package producer

import (
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
)

type Config struct {
	Id               string
	BootstrapServers []string
	Acks             RequiredAcks
	Transactional    struct {
		Enabled bool
		Id      string
	}
	Idempotent      bool
	Logger          log.Logger
	MetricsReporter metrics.Reporter
}

func NewConfig() *Config {
	return &Config{
		Acks:            WaitForAll,
		Logger:          log.NewNoopLogger(),
		MetricsReporter: metrics.NoopReporter(),
	}
}
