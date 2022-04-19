package tasks

import (
	"context"
	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/pkg/async"
	"github.com/gmbyapa/kstream/streams/topology"
	"github.com/tryfix/metrics"
)

type globalTask struct {
	*task
}

func (t *globalTask) Init(ctx topology.SubTopologyContext) error {
	t.metrics.processLatencyMicroseconds = t.metrics.reporter.Observer(metrics.MetricConf{
		Path:   `process_latency_microseconds`,
		Labels: []string{`topic_partition`},
	})

	defer func() {
		for _, store := range t.subTopology.StateStores() {
			stateStore := store
			t.runGroup.Add(func(opts *async.Opts) error {
				stateSynced := make(chan struct{}, 1)
				go func() {
					defer async.LogPanicTrace(t.logger)

					// Once the state is synced signal the RunGroup the process is ready
					<-stateSynced
					opts.Ready()
				}()

				go func() {
					defer async.LogPanicTrace(t.logger)

					<-opts.Stopping()
					if err := stateStore.Stop(); err != nil {
						panic(err.Error())
					}
				}()

				return stateStore.Sync(ctx, stateSynced)
			})
		}
	}()

	return t.subTopology.Init(ctx)
}

func (t *globalTask) Start(ctx context.Context, claim kafka.PartitionClaim, s kafka.GroupSession) {
	panic(`GlobalTask does not support processing`)
}

func (t *globalTask) Stop() error {
	t.runGroup.Stop()
	return nil
}
