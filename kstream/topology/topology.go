package topology

import (
	"github.com/tryfix/kstream/kstream/store"
)

type Topology interface {
	//Stores() store.Registry
	SubTopologies() []SubTopology
	Describe() string
}

type kTopology struct {
	stores       store.Registry
	streams      []SubTopology
	globalTables []SubTopology
}

func (k *kTopology) Stores() store.Registry {
	return k.stores
}

func (k *kTopology) SubTopologies() []SubTopology {
	return k.streams
}

func (k *kTopology) Describe() string {
	viz := NewTopologyVisualizer()
	viz.AddTopology(k)
	st, err := viz.Visualize()
	if err != nil {
		panic(err)
	}

	return st
}
