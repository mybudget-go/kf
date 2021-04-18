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
	"time"
)

type librdTxProducer struct {
	config        *Config
	librdProducer *kafka.Producer

	metrics struct {
		produceLatency      metrics.Observer
		batchProduceLatency metrics.Observer
	}
}

func (p *librdTxProducer) InitTransactions(ctx context.Context) error {
	return p.librdProducer.InitTransactions(ctx)
}

func (p *librdTxProducer) BeginTransaction() error {
	return p.librdProducer.BeginTransaction()
}

func (p *librdTxProducer) CommitTransaction(ctx context.Context) error {
	return p.librdProducer.CommitTransaction(ctx)
}

func (p *librdTxProducer) SendOffsetsToTransaction(ctx context.Context, offsets []producer.ConsumerOffset, meta string) error {
	var kOfsets []kafka.TopicPartition
	for _, o := range offsets {
		kOfsets = append(kOfsets, kafka.TopicPartition{
			Topic:     &o.Topic,
			Partition: o.Partition,
			Offset:    kafka.Offset(o.Offset),
			Metadata:  &meta,
		})
	}
	return p.librdProducer.SendOffsetsToTransaction(ctx, kOfsets, &kafka.ConsumerGroupMetadata{})
}

func (p *librdTxProducer) AbortTransaction(ctx context.Context) error {
	return p.librdProducer.AbortTransaction(ctx)
}

func NewTransactionalProducer(configs *Config) (producer.TransactionalProducer, error) {
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
	p := &librdTxProducer{
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

func (p *librdTxProducer) Close() error {
	cId, err := p.config.Librd.Get(`client.id`, nil)
	if err != nil {
		return err
	}
	defer p.config.Logger.Info(fmt.Sprintf(`saramaProducer [%s] closed`, cId.(string)))
	p.librdProducer.Close()
	return nil
}

func (p *librdTxProducer) Produce(ctx context.Context, message *data.Record) (partition int32, offset int64, err error) {
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

func (p *librdTxProducer) ProduceBatch(ctx context.Context, messages []*data.Record) error {
	panic(`implement me`)
}
