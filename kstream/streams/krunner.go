package streams

import (
	"context"
	"github.com/tryfix/kstream/consumer"
	"github.com/tryfix/kstream/kstream/topology"
	"github.com/tryfix/kstream/producer"
)

type Runner interface {
	topology.Topology
	Run(ctx context.Context) error
}

// run consumer and get allocation
// on re-balance start or stop the applications
type KRunner struct {
	topology      topology.Topology
	subTopologies map[string]topology.SubTopology
	builder       *StreamBuilder
}

func (K *KRunner) SubTopologies() []topology.SubTopology {
	return K.topology.SubTopologies()
}

func (K *KRunner) Describe() string {
	return K.topology.Describe()
}

func (K *KRunner) OnPartitionRevoked(ctx context.Context, session consumer.GroupSession) error {
	// stop all the producers TODO make use of sticky assigner

	for _, tp := range session.Claims() {
		for topic, sbTp := range K.subTopologies {
			for _, src := range sbTp.Sinks() {
				if topic == tp.Topic {
					if err := src.(*kSink).RemoveProducer(tp.Topic, tp.Partition); err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

func (K *KRunner) OnPartitionAssigned(ctx context.Context, session consumer.GroupSession) error {
	// get the assign vs revoked diff
	// enable transactional producers ????
	// stop sink producers on revoked partitions
	// add new producers on newly added partitions
	//
	for _, tp := range session.Claims() {
		for topic, sbTp := range K.subTopologies {
			for _, src := range sbTp.Sinks() {
				if topic == tp.Topic {
					if err := src.(*kSink).AddProducer(tp.Topic, tp.Partition); err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

func (K *KRunner) Consume(ctx context.Context, session consumer.GroupSession, tp consumer.PartitionClaim) error {
	// get a sub topology
	sTp := K.subTopologies[tp.Topic()]
	var src *KSource
	for _, source := range sTp.Sources() {
		if source.(*KSource).Topic() == tp.Topic() {
			src = source.(*KSource)
			break
		}
	}

	task := &kTask{
		id:     newTaskId(tp.Topic(), tp.Partition()),
		source: src,
		transactional: struct {
			enabled  bool
			producer producer.TransactionalProducer
		}{},
	}

	for message := range tp.Messages() {
		if err := task.Run(message); err != nil {
			println(`errrrrrrr`, err)
		}
	}

	return nil
}

func (K *KRunner) Run(ctx context.Context) error {

	// start global tables

	// start a consumer
	c, err := K.builder.defaultBuilders.GroupConsumer(func(config *consumer.Config) {

	})
	if err != nil {
		return err
	}

	K.setUpSubTopologies(K.topology)

	tps, err := K.topics()
	if err != nil {
		return err
	}

	if err := c.Subscribe(ctx, tps, K); err != nil {
		return err
	}

	return nil
}

func (K *KRunner) topics() ([]string, error) {
	var tps []string
	for topic := range K.subTopologies {
		tps = append(tps, topic)
	}

	return tps, nil
}

func (K *KRunner) setUpSubTopologies(tp topology.Topology) {
	for _, sbtp := range tp.SubTopologies() {
		for _, src := range sbtp.Sources() {
			K.subTopologies[src.(*KSource).topic] = sbtp
		}
	}
}
