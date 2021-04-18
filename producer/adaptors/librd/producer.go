/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package librd

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/tryfix/errors"
	"github.com/tryfix/kstream/data"
	"github.com/tryfix/kstream/producer"
	"github.com/tryfix/metrics"
	"strings"
	"time"
)

type librdProducer struct {
	config        *Config
	librdProducer *kafka.Producer

	metrics struct {
		produceLatency      metrics.Observer
		batchProduceLatency metrics.Observer
	}
}

func NewProducerBuilder(config func(*Config)) producer.Builder {
	defaultConf := NewConfig()
	if config != nil {
		config(defaultConf)
	}

	return func(conf func(*producer.Config)) (producer.Producer, error) {
		conf(defaultConf.Config)
		if err := defaultConf.Librd.SetKey(`client.id`, defaultConf.Id); err != nil {
			panic(err)
		}

		if err := defaultConf.Librd.SetKey(`bootstrap.servers`, strings.Join(defaultConf.BootstrapServers, `,`)); err != nil {
			panic(err)
		}

		if defaultConf.Transactional.Enabled {
			if err := defaultConf.Librd.SetKey(`enable.idempotence`, true); err != nil {
				panic(err)
			}

			if err := defaultConf.Librd.SetKey(`transactional.id`, defaultConf.Transactional.Id); err != nil {
				panic(err)
			}

			if err := defaultConf.Librd.SetKey(`max.in.flight.requests.per.connection`, 1); err != nil {
				panic(err)
			}

			if err := defaultConf.Librd.SetKey(`acks`, `all`); err != nil {
				panic(err)
			}

			return NewTransactionalProducer(defaultConf)
		}

		return NewProducer(defaultConf)
	}
}

func NewProducer(configs *Config) (producer.Producer, error) {
	if err := configs.validate(); err != nil {
		return nil, err
	}

	configs.Logger.Info(`saramaProducer [` + configs.Id + `] initiating...`)
	prd, err := kafka.NewProducer(configs.Librd)
	if err != nil {
		return nil, errors.WithPrevious(err, fmt.Sprintf(`[%s] init failed`, configs.Id))
	}

	defer configs.Logger.Info(`saramaProducer [` + configs.Id + `] initiated`)

	labels := []string{`topic`, `partition`}
	p := &librdProducer{
		config:        configs,
		librdProducer: prd,
	}

	p.metrics.produceLatency = configs.MetricsReporter.Observer(metrics.MetricConf{
		Path:        `k_stream_producer_produced_latency_microseconds`,
		Labels:      labels,
		ConstLabels: map[string]string{`producer_id`: configs.Id},
	})

	p.metrics.batchProduceLatency = configs.MetricsReporter.Observer(metrics.MetricConf{
		Path:        `k_stream_producer_batch_produced_latency_microseconds`,
		Labels:      append(labels, `size`),
		ConstLabels: map[string]string{`producer_id`: configs.Id},
	})

	return p, nil
}

func (p *librdProducer) Close() error {
	cId, err := p.config.Librd.Get(`client.id`, nil)
	if err != nil {
		return err
	}
	defer p.config.Logger.Info(fmt.Sprintf(`saramaProducer [%s] closed`, cId.(string)))
	p.librdProducer.Close()
	return nil
}

func (p *librdProducer) Produce(ctx context.Context, message *data.Record) (partition int32, offset int64, err error) {
	t := time.Now()

	m := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic: &message.Topic,
		},
		Key:           message.Key,
		Value:         message.Value,
		Timestamp:     t,
		TimestampType: kafka.TimestampCreateTime,
	}

	for _, header := range message.Headers.All() {
		m.Headers = append(m.Headers, kafka.Header{
			Key:   string(header.Key),
			Value: header.Value,
		})
	}

	if !message.Timestamp.IsZero() {
		m.Timestamp = message.Timestamp
	}

	if message.Partition > 0 {
		m.TopicPartition.Partition = message.Partition
	}

	dChan := make(chan kafka.Event)
	err = p.librdProducer.Produce(m, dChan)
	if err != nil {
		return 0, 0, errors.WithPrevious(err, `cannot send message`)
	}

	dRpt := <-dChan
	dmSg := dRpt.(*kafka.Message)

	if dmSg.TopicPartition.Error != nil {
		return 0, 0, errors.WithPrevious(err, `message delivery failed`)
	}

	p.metrics.produceLatency.Observe(float64(time.Since(t).Nanoseconds()/1e3), map[string]string{
		`topic`:     *dmSg.TopicPartition.Topic,
		`partition`: fmt.Sprint(dmSg.TopicPartition.Partition),
	})

	p.config.Logger.TraceContext(ctx, fmt.Sprintf("Delivered message to topic %s [%d] at offset %d",
		message.Topic, dmSg.TopicPartition.Partition, dmSg.TopicPartition.Offset))

	return dmSg.TopicPartition.Partition, int64(dmSg.TopicPartition.Offset), nil
}

func (p *librdProducer) ProduceBatch(ctx context.Context, messages []*data.Record) error {
	panic(`implement me`)
}
