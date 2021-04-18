/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package streams

import (
	"github.com/tryfix/errors"
	"github.com/tryfix/kstream/consumer"
	"github.com/tryfix/kstream/producer"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
	"time"
)

type KafkaVersion string

type ProcessingGuarantee int8

const (
	AtLeastOnce ProcessingGuarantee = iota
	ExactlyOnce
)

type Config struct {
	// ApplicationId will be used as
	// 	1 - a consumer group when using streams
	// 	2 - topic prefix when creating repartition topics
	// 	3 - transaction id prefix when using producer transactions
	ApplicationId    string
	BootstrapServers []string // kafka Brokers
	KafkaVersion     string
	Store            struct {
		Http struct {
			Enabled bool
			Host    string
		}
		StateDir string
	}
	Processing struct {
		Guarantee ProcessingGuarantee
	}
	Consumer struct {
		BootstrapServers []string
		IsolationLevel   consumer.IsolationLevel
		Commit           struct {
			Auto     bool
			Interval time.Duration
		}
	}
	Host     string
	Producer struct {
		Transactional    bool
		BootstrapServers []string
		RequiredAcks     producer.RequiredAcks
		Idempotent       bool
	}
	KafkaLogsEnabled bool
	MetricsReporter  metrics.Reporter
	Logger           log.Logger
	DefaultBuilders  DefaultBuilders
}

func NewStreamBuilderConfig() *Config {
	config := &Config{}
	if config.Processing.Guarantee == ExactlyOnce {
		config.Consumer.IsolationLevel = consumer.ReadCommitted
		config.Producer.RequiredAcks = producer.WaitForAll
		config.Producer.Idempotent = true
		config.Producer.Transactional = true
		config.Consumer.Commit.Auto = false
	}

	config.Producer.RequiredAcks = producer.WaitForAll
	config.KafkaLogsEnabled = false

	// default metrics reporter
	config.MetricsReporter = metrics.NoopReporter()
	config.Logger = log.NewNoopLogger()

	return config
}

func (c *Config) validate() error {
	c.Logger = c.Logger.NewLog(log.Prefixed(`k-stream`))

	if c.ApplicationId == `` {
		return errors.New(`[ApplicationId] cannot be empty`)
	}

	if len(c.BootstrapServers) < 1 {
		return errors.New(`[BootstrapServers] cannot be empty`)
	}

	if c.Processing.Guarantee == ExactlyOnce {
		if c.Consumer.IsolationLevel != consumer.ReadCommitted {
			return errors.New(`[Consumer.IsolationLevel] needs to be ReadCommitted 
					when Processing.Guarantee == ExactlyOnce`)
		}

		if c.Producer.RequiredAcks != producer.WaitForAll {
			return errors.New(`[Producer.RequiredAcks] needs to be WaitForAll 
					when Processing.Guarantee == ExactlyOnce`)
		}

		if !c.Producer.Idempotent {
			return errors.New(`[Producer.Idempotent] needs to be true 
					when Processing.Guarantee == ExactlyOnce`)
		}

		if !c.Producer.Transactional {
			return errors.New(`[Producer.Transactional] needs to be true 
					when Processing.Guarantee == ExactlyOnce`)
		}

		if !c.Consumer.Commit.Auto {
			return errors.New(`[Consumer.Commit.Auto] needs to be false 
					when Processing.Guarantee == ExactlyOnce`)
		}
	}

	return nil
}
