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
	librdKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/pkg/errors"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
	"time"
)

const (
	PartitionerRandom                  kafka.PartitionerType = `random`
	PartitionerCRC32                   kafka.PartitionerType = `consistent`
	PartitionerCRC32Random             kafka.PartitionerType = `consistent_random`
	PartitionerConsistentMurmur2       kafka.PartitionerType = `murmur2`
	PartitionerConsistentMurmur2Random kafka.PartitionerType = `murmur2_random`
	PartitionerConsistentFNV1a         kafka.PartitionerType = `fnv1a`
	PartitionerConsistentFNV1aRandom   kafka.PartitionerType = `fnv1a_random`
)

type librdProducer struct {
	config        *ProducerConfig
	librdProducer *librdKafka.Producer

	metrics struct {
		produceLatency      metrics.Observer
		batchProduceLatency metrics.Observer
	}
}

type producerProvider struct {
	config *ProducerConfig
}

func NewProducerProvider(config *ProducerConfig) kafka.ProducerProvider {
	return &producerProvider{config: config}
}

func (c *producerProvider) NewBuilder(conf *kafka.ProducerConfig) kafka.ProducerBuilder {
	c.config.ProducerConfig = conf

	if err := c.config.Librd.SetKey(`go.events.channel.size`, 1000); err != nil {
		panic(err)
	}

	if err := c.config.Librd.SetKey(`go.produce.channel.size`, 1000); err != nil {
		panic(err)
	}

	return func(configure func(*kafka.ProducerConfig)) (kafka.Producer, error) {
		defaultConfCopy := c.config.copy()
		configure(defaultConfCopy.ProducerConfig)

		return NewProducer(defaultConfCopy)
	}
}

/*func NewProducerAdaptor(configure func(*ProducerConfig)) kafka.ProducerBuilder {
	defaultConf := NewProducerConfig()
	configure(defaultConf)

	return func(configure func(*kafka.ProducerConfig)) (kafka.Producer, error) {
		defaultConfCopy := defaultConf.copy()
		configure(defaultConfCopy.ProducerConfig)
		if err := defaultConfCopy.Librd.SetKey(`client.id`, defaultConfCopy.Id); err != nil {
			panic(err)
		}

		if err := defaultConfCopy.Librd.SetKey(`bootstrap.servers`, strings.Join(defaultConfCopy.BootstrapServers, `,`)); err != nil {
			panic(err)
		}

		if err := defaultConfCopy.Librd.SetKey(`go.logs.channel.enable`, true); err != nil {
			return nil, errors.New(err.Error())
		}

		// Making sure transactional properties are set
		if defaultConfCopy.Transactional.Enabled {
			if err := defaultConfCopy.Librd.SetKey(`enable.idempotence`, true); err != nil {
				panic(err)
			}

			// For transactional producers, delivery success is
			// acknowledged by producer batch commit, so we don't need
			// to listen to individual delivery reports
			if err := defaultConfCopy.Librd.SetKey(`go.delivery.reports`, false); err != nil {
				panic(err)
			}

			// TODO use this to recreate producer failure scenarios
			//if err := defaultConfCopy.Librd.SetKey(`transaction.timeout.ms`, 1000); err != nil{
			//	panic(err)
			//}

			if err := defaultConfCopy.Librd.SetKey(`transactional.id`, defaultConfCopy.Transactional.Id); err != nil {
				panic(err)
			}

			if err := defaultConfCopy.Librd.SetKey(`max.in.flight.requests.per.connection`, 1); err != nil {
				panic(err)
			}

			if err := defaultConfCopy.Librd.SetKey(`acks`, `all`); err != nil {
				panic(err)
			}
		}

		return NewProducer(defaultConfCopy)
	}
}*/

func NewProducer(configs *ProducerConfig) (kafka.Producer, error) {
	if err := configs.setUp(); err != nil {
		return nil, errors.Wrap(err, `producer configs setup failed`)
	}

	if err := configs.validate(); err != nil {
		return nil, errors.Wrap(err, `invalid producer configs`)
	}

	loggerPrefix := `Producer`
	if configs.Transactional.Enabled {
		loggerPrefix = `TransactionalProducer`
	}

	configs.Logger = configs.Logger.NewLog(log.Prefixed(fmt.Sprintf(`%s(librdkafka)`, loggerPrefix)))

	configs.Logger.Info(`Producer initiating...`)
	producer, err := librdKafka.NewProducer(configs.Librd)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf(`[%s] init failed`, configs.Id))
	}

	defer configs.Logger.Info(`Producer initiated`)

	labels := []string{`topic`, `partition`}
	p := &librdProducer{
		config:        configs,
		librdProducer: producer,
	}

	// Drain all the delivery messages (we don't need them here. since we are relying on transaction commit).
	// This is important because if Produce() failed for a batch, the failed delivery reports will be sent to this
	// channel
	go func() {
		for ev := range producer.Events() {
			switch e := ev.(type) {
			case librdKafka.Error:
				p.config.Logger.Error(fmt.Sprintf(`Event [%s]%s`, e.Code(), e))
			case *librdKafka.Message:
				p.config.Logger.Error(fmt.Sprintf(`Event %s`, e.TopicPartition.Error))
			}
		}
	}()

	go p.printLogs()

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

	if p.config.Transactional.Enabled {
		return &librdTxProducer{p}, nil
	}

	return p, nil
}

func (p *librdProducer) NewRecord(
	ctx context.Context,
	key []byte,
	value []byte,
	topic string,
	partition int32,
	timestamp time.Time,
	headers kafka.RecordHeaders,
	meta string) kafka.Record {

	librdHeaders := make([]librdKafka.Header, len(headers))
	for i := range headers {
		librdHeaders[i] = librdKafka.Header{
			Key:   string(headers[i].Key),
			Value: headers[i].Value,
		}
	}

	return &Record{
		librd: &librdKafka.Message{
			TopicPartition: librdKafka.TopicPartition{
				Topic:     &topic,
				Partition: partition,
				Metadata:  &meta,
			},
			Value:     value,
			Key:       key,
			Timestamp: timestamp,
			Headers:   librdHeaders,
		},
		ctx: ctx,
	}
}

func (p *librdProducer) ProduceSync(ctx context.Context, message kafka.Record) (partition int32, offset int64, err error) {
	dChan := make(chan librdKafka.Event)
	kMessage := p.prepareMessage(message)
	err = p.librdProducer.Produce(kMessage, dChan)
	if err != nil {
		return 0, 0, errors.Wrap(err, `cannot send message`)
	}

	dRpt := <-dChan
	dmSg := dRpt.(*librdKafka.Message)

	if dmSg.TopicPartition.Error != nil {
		return 0, 0, errors.Wrapf(dmSg.TopicPartition.Error, `message %s delivery failed`, message)
	}

	p.metrics.produceLatency.Observe(float64(time.Since(kMessage.Timestamp).Nanoseconds()/1e3), map[string]string{
		`topic`:     *dmSg.TopicPartition.Topic,
		`partition`: fmt.Sprint(dmSg.TopicPartition.Partition),
	})

	p.config.Logger.DebugContext(ctx, fmt.Sprintf("Delivered message to topic %s[%d]@%d",
		message.Topic(), dmSg.TopicPartition.Partition, dmSg.TopicPartition.Offset))

	return dmSg.TopicPartition.Partition, int64(dmSg.TopicPartition.Offset), nil
}

func (p *librdProducer) Close() error {
	p.config.Logger.Info(`Producer closing...`)
	defer p.config.Logger.Info(`Producer closed`)

	// Lets remove all the messages in librdkafka queues
	if err := p.librdProducer.Purge(
		librdKafka.PurgeInFlight |
			librdKafka.PurgeNonBlocking | librdKafka.PurgeQueue); err != nil {
		p.config.Logger.Error(err)
	}

	err := p.librdProducer.AbortTransaction(nil)
	if err != nil {
		if err.(librdKafka.Error).Code() == librdKafka.ErrState {
			// No transaction in progress, ignore the error.
			err = nil
		} else {
			return err
		}
	}

	p.librdProducer.Close()

	return nil
}

func (p *librdProducer) Restart() error {
	p.config.Logger.Info(`Producer restarting...`)
	defer p.config.Logger.Info(`Producer restarted`)

	if err := p.forceClose(); err != nil {
		p.config.Logger.Warn(err)
	}

	prd, err := librdKafka.NewProducer(p.config.Librd)
	if err != nil {
		p.config.Logger.Fatal(err)
	}

	p.librdProducer = prd

	return nil
}

func (p *librdProducer) printLogs() {
	logger := p.config.Logger.NewLog(log.Prefixed(`LibrdLogs`))
	for lg := range p.librdProducer.Logs() {
		switch lg.Level {
		case 0, 1, 2:
			logger.Error(lg.String(), `level`, lg.Level)
		case 3, 4, 5:
			logger.Warn(lg.String(), `level`, lg.Level)
		case 6:
			logger.Info(lg.String(), `level`, lg.Level)
		case 7:
			logger.Debug(lg.String(), `level`, lg.Level)
		}
	}
}

func (p *librdProducer) forceClose() error {
	p.config.Logger.Error(`Producer closing forcefully...`) // TODO change log level
	defer p.config.Logger.Error(`Producer closed`)          // TODO change log level

	p.config.Logger.Info(`Purging producer queues kafka.PurgeInFlight|kafka.PurgeNonBlocking|kafka.PurgeQueue`)
	if err := p.librdProducer.Purge(
		librdKafka.PurgeInFlight |
			librdKafka.PurgeNonBlocking | librdKafka.PurgeQueue); err != nil {
		p.config.Logger.Error(err)
	}

	err := p.librdProducer.AbortTransaction(nil)
	if err != nil {
		if err.(librdKafka.Error).Code() == librdKafka.ErrState {
			// No transaction in progress, ignore the error.
			err = nil
		} else {
			return err
		}
	}

	//p.librdProducer.Flush(10000) TODO fix this

	p.librdProducer.Close()

	return nil
}

func (p *librdProducer) prepareMessage(message kafka.Record) *librdKafka.Message {
	t := time.Now()
	topic := message.Topic()
	m := &librdKafka.Message{
		TopicPartition: librdKafka.TopicPartition{
			Topic: &topic,
		},
		Key:           message.Key(),
		Value:         message.Value(),
		Timestamp:     t,
		TimestampType: librdKafka.TimestampCreateTime,
	}

	// Use the partitioner defined in librdkafka config
	if message.Partition() == kafka.PartitionAny {
		m.TopicPartition.Partition = librdKafka.PartitionAny
	}

	if p.config.PartitionerFunc != nil {
		m.TopicPartition.Partition, _ = p.config.PartitionerFunc(message, 1) // TODO fix this
	}

	for _, header := range message.Headers() {
		m.Headers = append(m.Headers, librdKafka.Header{
			Key:   string(header.Key),
			Value: header.Value,
		})
	}

	if !message.Timestamp().IsZero() {
		m.Timestamp = message.Timestamp()
	}

	if message.Partition() > 0 {
		m.TopicPartition.Partition = message.Partition()
	}

	return m
}
