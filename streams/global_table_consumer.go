package streams

import (
	"context"
	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/pkg/async"
	"github.com/gmbyapa/kstream/pkg/errors"
	"github.com/gmbyapa/kstream/streams/tasks"
	"github.com/gmbyapa/kstream/streams/topology"
	"github.com/tryfix/log"
)

type GlobalTableConsumer struct {
	consumer    kafka.PartitionConsumer
	logger      log.Logger
	taskManager tasks.TaskManager
	ctx         topology.BuilderContext
	runGroup    *async.RunGroup
}

func (g *GlobalTableConsumer) Init(topologyBuilder topology.Topology) error {
	for _, builder := range topologyBuilder.GlobalTableTopologies() {
		topic := builder.Sources()[0].Topic()
		partitions, err := g.consumer.Partitions(context.Background(), topic)
		if err != nil {
			return errors.Wrapf(err, `cannot fetch partition for topic [%s]`, topic)
		}

		for _, partition := range partitions {
			taskId := g.taskManager.NewTaskId(`Global`, kafka.TopicPartition{
				Topic:     topic,
				Partition: partition,
			})
			task, err := g.taskManager.AddGlobalTask(g.ctx, taskId, builder)
			if err != nil {
				return errors.Wrapf(err, `global task[%s] add failed, topic: %s`, taskId, topic)
			}

			g.runGroup.Add(func(opts *async.Opts) error {
				go func(task tasks.Task) {
					defer async.LogPanicTrace(g.logger)

					<-opts.Stopping()
					if err := task.Stop(); err != nil {
						panic(err.Error())
					}
				}(task)

				go func(task tasks.Task) {
					defer async.LogPanicTrace(g.logger)

					if err := task.Ready(); err != nil && !errors.Is(err, async.ErrInterrupted) {
						g.logger.Error(err.Error())
					}
					opts.Ready()
				}(task)

				return task.Sync()
			})
		}
	}

	return nil
}

func (g *GlobalTableConsumer) Run(_ topology.Topology) error {
	g.logger.Info(`Starting...`)
	return g.runGroup.Run()
}

func (g *GlobalTableConsumer) Ready() error {
	defer g.logger.Info(`Started`)
	return g.runGroup.Ready()
}

func (g *GlobalTableConsumer) Stop() error {
	g.runGroup.Stop()
	return nil
}
