package streams

import (
	"fmt"
	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/streams/topology"
)

type KSubTopology struct {
	//root     topology.Node
	id       topology.SubTopologyId
	edges    []topology.Edge
	nodes    []topology.Node
	sources  []topology.Source
	sinks    []topology.Sink
	producer kafka.Producer
	stores   map[string]topology.StateStore
}

func (t *KSubTopology) Source(topic string) topology.Source {
	for _, src := range t.sources {
		if src.Topic() == topic {
			return src
		}
	}

	return nil
}

func (t *KSubTopology) Sinks() []topology.Sink {
	return t.sinks
}

func (t *KSubTopology) Id() topology.SubTopologyId {
	return t.id
}

func (t *KSubTopology) AddProducer(p kafka.Producer) {
	t.producer = p
}

func (t *KSubTopology) Producer() kafka.Producer {
	return t.producer
}

func (t *KSubTopology) Store(name string) topology.StateStore {
	stor, ok := t.stores[name]
	if !ok {
		panic(fmt.Sprintf(`store [%s], does not exist`, name))
	}

	return stor
}

func (t *KSubTopology) AddStore(store topology.LoggableStateStore) {
	if _, ok := t.stores[store.Name()]; ok {
		panic(fmt.Sprintf(`store [%s], already exists`, store.Name()))
	}

	t.stores[store.Name()] = store
}

func (t *KSubTopology) Nodes() []topology.Node {
	return t.nodes
}

func (t *KSubTopology) StateStores() map[string]topology.StateStore {
	return t.stores
}

func (t *KSubTopology) Init(ctx topology.SubTopologyContext) error {
	for _, nd := range t.nodes {
		if ndInt, ok := nd.(topology.InitableNode); ok {
			// create a node context
			nodeCtx := topology.NewNodeContext(ctx, t)
			if err := ndInt.Init(nodeCtx); err != nil {
				return err
			}
		}
	}

	return nil
}

func (t *KSubTopology) Destroy() error {
	for _, nd := range t.nodes {
		if ndInt, ok := nd.(topology.DestroyableNode); ok {
			if err := ndInt.Destroy(); err != nil {
				return err
			}
		}
	}

	return nil
}
