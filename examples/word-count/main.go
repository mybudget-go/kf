package main

import (
	"context"
	"fmt"
	"github.com/bxcodec/faker/v3"
	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/kafka/adaptors/librd"
	"github.com/gmbyapa/kstream/streams"
	"github.com/gmbyapa/kstream/streams/encoding"
	"github.com/tryfix/log"
	"os"
	"os/signal"
	"strings"
	"time"
)

func main() {
	config := streams.NewStreamBuilderConfig()
	config.BootstrapServers = []string{`192.168.0.101:9092`}
	config.ApplicationId = `word-count`
	config.Consumer.Offsets.Initial = kafka.Earliest
	config.Logger = log.Constructor.Log()

	seed()

	return

	builder := streams.NewStreamBuilder(config)
	buildTopology(builder)

	topology, err := builder.Build()
	if err != nil {
		panic(err)
	}

	println(topology.Describe())

	runner := builder.NewRunner()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt)

	go func() {
		<-sigs
		if err := runner.Stop(); err != nil {
			println(err)
		}
	}()

	if err := runner.Run(topology); err != nil {
		panic(err)
	}
}

func buildTopology(builder *streams.StreamBuilder) {
	stream := builder.KStream(`textlines`, encoding.StringEncoder{}, encoding.StringEncoder{})
	stream.Each(func(ctx context.Context, key, value interface{}) {
		println(`Word count for : ` + value.(string))
	}).FlatMapValues(func(ctx context.Context, key, value interface{}) (values []interface{}, err error) {
		for _, word := range strings.Split(value.(string), ` `) {
			values = append(values, word)
		}
		return
	}).SelectKey(func(ctx context.Context, key, value interface{}) (kOut interface{}, err error) {
		return value, nil
	}).Repartition(`textlines-by-word`).Aggregate(`word-count`, func(ctx context.Context, key, value, previous interface{}) (newAgg interface{}, err error) {
		var count int
		if previous != nil {
			count = previous.(int)
		}
		count++
		newAgg = count

		return
	}, streams.AggregateWithValEncoder(encoding.IntEncoder{})).ToStream().To(`word-counts`)
}

func seed() {
	conf := librd.NewProducerConfig()
	conf.BootstrapServers = []string{`192.168.0.101:9092`}
	producer, err := librd.NewProducer(conf)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 1; i++ {
		record := producer.NewRecord(
			context.Background(),
			[]byte(`ss`),
			[]byte(faker.Sentence()),
			`mos.accounts`,
			kafka.PartitionAny,
			time.Now(),
			nil,
			``,
		)
		p, o, err := producer.ProduceSync(context.Background(), record)
		if err != nil {
			panic(err)
		}

		println(fmt.Sprintf(`message produced to textlines[%d]@%d`, p, o))
	}
}
