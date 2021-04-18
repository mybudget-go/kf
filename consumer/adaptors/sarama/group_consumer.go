package sarama

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/tryfix/errors"
	"github.com/tryfix/kstream/consumer"
	"time"
)

type groupConsumer struct {
	group          sarama.ConsumerGroup
	config         *Config
	consumerErrors chan error
	stopping       chan bool
	stopped        chan bool
}

func NewGroupConsumerAdaptor(configure func(*Config)) consumer.GroupConsumerBuilder {
	defaultConf := NewGroupConsumerConfig()
	configure(defaultConf)
	return func(configure func(*consumer.Config)) (consumer.Consumer, error) {
		//defaultConf := consumer.NewConfig()
		configure(defaultConf.Config)
		defaultConf.Sarama.ClientID = defaultConf.Id
		defaultConf.Sarama.Consumer.IsolationLevel = sarama.IsolationLevel(defaultConf.IsolationLevel)

		return NewGroupConsumer(defaultConf)
	}
}

//func NewGroupConsumerBuilder(config *Config) consumer.GroupConsumerBuilder  {
//	return func(opts ...consumer.Option) (consumer.Consumer, error) {
//		options := new(consumer.Options)
//		options.ApplyDefault()
//		options.Apply(opts...)
//		config.options = options
//		return NewGroupConsumer(config)
//	}
//}

func NewGroupConsumer(config *Config) (consumer.Consumer, error) {
	group, err := sarama.NewConsumerGroup(config.BootstrapServers, config.GroupId, config.Sarama)
	if err != nil {
		return nil, errors.WithPrevious(err, "Failed to create consumer")
	}

	return &groupConsumer{
		group:          group,
		config:         config,
		consumerErrors: make(chan error, 1),
		stopping:       make(chan bool, 1),
		stopped:        make(chan bool, 1),
	}, nil
}

func (g *groupConsumer) Subscribe(ctx context.Context, tps []string, handler consumer.GroupHandler) error {
	gHandler := new(groupHandler)
	gHandler.handler = handler
	gHandler.logger = g.config.Logger
	gHandler.recordUuidExtractFunc = g.config.RecordUuidExtractFunc
	gHandler.metrics.reporter = g.config.MetricsReporter
	gHandler.setUpMetrics()

CLoop:
	for {
		if err := g.group.Consume(ctx, tps, gHandler); err != nil && err != sarama.ErrClosedConsumerGroup {
			t := 2 * time.Second
			g.config.Logger.Error(fmt.Sprintf(`consumer err (%s) while consuming. retrying in %s`, err, t.String()))
			time.Sleep(t)
			continue CLoop
		}

		select {
		case <-ctx.Done():

			g.config.Logger.Info(fmt.Sprintf(`stopping consumer due to %s`, ctx))
			break CLoop
		default:
			continue CLoop
		}
	}

	g.stopped <- true

	// close consumer group
	if err := g.group.Close(); err != nil {
		g.config.Logger.Error(`k-stream.consumer`,
			fmt.Sprintf(`cannot close consumer due to %+v`, err))
	}

	g.config.Logger.Info(`consumer stopped`)
	gHandler.cleanUpMetrics()

	return nil
}

func (g *groupConsumer) Errors() <-chan error {
	return g.consumerErrors
}
