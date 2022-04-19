package streams

import (
	"context"
	"fmt"
	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/streams/tasks"
	"github.com/gmbyapa/kstream/streams/topology"
	"github.com/tryfix/log"
	"sync"
)

type streamConsumerInstance struct {
	topologyBuilder   topology.Topology
	generation        tasks.TaskAssignment
	currentAssignment tasks.TaskAssignment
	ctx               topology.BuilderContext

	logger log.Logger

	taskManager tasks.TaskManager
	consumer    kafka.GroupConsumer
}

func (r *streamConsumerInstance) OnPartitionRevoked(ctx context.Context, session kafka.GroupSession) error {
	r.logger.Info(fmt.Sprintf("Removing tasks -> \n%s", r.currentAssignment))
	wg := sync.WaitGroup{}
	wg.Add(len(r.currentAssignment))
	for _, mapping := range r.currentAssignment {
		go func(mapping *tasks.TaskMapping) {
			defer wg.Done()
			if err := r.taskManager.RemoveTask(mapping.TaskId()); err != nil {
				r.logger.Fatal(err)
			}
		}(mapping)
	}
	wg.Wait()

	return nil
}

func (r *streamConsumerInstance) OnPartitionAssigned(ctx context.Context, session kafka.GroupSession) error {
	r.currentAssignment = r.generation.FindMappingsByTPs(session.Assignment()...)
	r.logger.Info(fmt.Sprintf("Assigning tasks -> \n%s", r.currentAssignment))
	wg := sync.WaitGroup{}
	wg.Add(len(r.currentAssignment))
	for _, mapping := range r.currentAssignment {
		go func(wg *sync.WaitGroup, mapping *tasks.TaskMapping) {
			defer wg.Done()
			t, err := r.taskManager.AddTask(r.ctx, mapping.TaskId(), mapping.SubTopologyBuilder(), session)
			if err != nil {
				panic(err)
			}

			if err := t.Sync(); err != nil {
				panic(err)
			}

		}(&wg, mapping)
	}
	wg.Wait()

	return nil
}

func (r *streamConsumerInstance) OnLost() error {
	r.logger.Info(`Consumer assignment lost`)
	return nil
}

func (r *streamConsumerInstance) Consume(ctx context.Context, session kafka.GroupSession, claim kafka.PartitionClaim) error {
	r.logger.Info(fmt.Sprintf(`Consuming messages for %s`, claim.TopicPartition()))
	defer r.logger.Info(fmt.Sprintf(`Consuming messages stopped for %s`, claim.TopicPartition()))

	mapping := r.generation.FindMappingByTP(claim.TopicPartition())
	task, err := r.taskManager.Task(mapping.TaskId())
	if err != nil {
		panic(err.Error())
	}

	task.Start(ctx, claim, session)

	return nil
}

func (r *streamConsumerInstance) Subscribe() error {
	if err := r.consumer.Subscribe(r.topologyBuilder.StreamTopologies().SourceTopics(), r); err != nil {
		r.logger.Error(err)
		return err
	}

	return nil
}

func (r *streamConsumerInstance) Unsubscribe() error {
	if err := r.consumer.Unsubscribe(); err != nil {
		return err
	}

	r.logger.Info(`Consumer unsubscribing...`)

	return nil
}
