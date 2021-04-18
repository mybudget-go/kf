package streams

import (
	"context"
	"github.com/tryfix/kstream/kstream/store"
	"github.com/tryfix/kstream/kstream/topology"
)

type GTableStore struct {
	store        store.Store
	storeBuilder store.Builder
}

func (s *GTableStore) Build() (topology.Node, error) {
	panic("implement me")
}

func (s *GTableStore) Id() topology.NodeId {
	panic("implement me")
}

func (s *GTableStore) SetId(id topology.NodeId) {
	panic("implement me")
}

func (s *GTableStore) Type() topology.Type {
	panic("implement me")
}

func (s *GTableStore) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
	return nil, nil, false, s.store.Set(ctx, kIn, vIn, 0)
}

func (s *GTableStore) AddEdge(node topology.Node) {
	panic("implement me")
}

func (s *GTableStore) Edges() []topology.Node {
	panic("implement me")
}
