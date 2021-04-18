package main

import (
	"context"
	"github.com/tryfix/kstream/kstream/processors"
	"github.com/tryfix/kstream/kstream/serdes"
	"github.com/tryfix/kstream/kstream/streams"
	"github.com/tryfix/log"
	"os"
	"os/signal"
)

func main() {
	logger := log.Constructor.Log(log.WithLevel(log.TRACE))
	conf := streams.NewStreamBuilderConfig()
	conf.BootstrapServers = []string{`localhost:9092`}
	conf.ApplicationId = `test_application`
	conf.Logger = logger
	builder := streams.NewStreamBuilder(conf)
	users := builder.KStream(`users`, serdes.StringSerializer{}, serdes.StringSerializer{})
	filtered := users.Filter(func(ctx context.Context, key, value interface{}) (bool, error) {
		println(`users.filter1`)
		return true, nil
	})
	users.Filter(func(ctx context.Context, key, value interface{}) (bool, error) {
		println(`users.filter2`)
		return true, nil
	})
	filtered.Filter(func(ctx context.Context, key, value interface{}) (bool, error) {
		println(`users.filter1.filter`)
		return true, nil
	})

	orders := builder.KStream(`orders`, serdes.StringSerializer{}, serdes.StringSerializer{})
	orderFiltered := orders.Filter(func(ctx context.Context, key, value interface{}) (bool, error) {
		println(`orders.filter1`)
		return true, nil
	})
	orderFiltered.Filter(func(ctx context.Context, key, value interface{}) (bool, error) {
		println(`orders.filter1.filter`)
		return true, nil
	})

	branches := orderFiltered.Branch(
		processors.BranchDetails{
			Name: "test branch one",
			Predicate: func(ctx context.Context, key interface{}, val interface{}) (bool, error) {
				return true, nil
			},
		},
		processors.BranchDetails{
			Name: "test branch two",
			Predicate: func(ctx context.Context, key interface{}, val interface{}) (bool, error) {
				return true, nil
			},
		})

	br1 := branches[0]
	br1.Filter(func(ctx context.Context, key, value interface{}) (bool, error) {
		println(`orders.filter1.br1.filter`)
		return true, nil
	})

	br2 := branches[1]
	br2.Filter(func(ctx context.Context, key, value interface{}) (bool, error) {
		println(`orders.filter1.br2.filter`)
		return true, nil
	})

	br2.To(`orders`, serdes.StringSerializer{}, serdes.StringSerializer{})

	br1.Through(`orders_combined`, serdes.StringSerializer{}, serdes.StringSerializer{}).Filter(func(ctx context.Context, key, value interface{}) (bool, error) {
		println(`orders_combined.filter`)
		return true, nil
	})

	tp := builder.Build(users, orders)
	println(tp.Describe())

	ctx, cancel := context.WithCancel(context.Background())

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)

	go func() {
		<-sig
		cancel()
	}()

	if err := tp.Run(ctx); err != nil {
		log.Fatal(err)
	}

}
