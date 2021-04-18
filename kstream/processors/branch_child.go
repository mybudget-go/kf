package processors

import (
	"context"
	"github.com/tryfix/kstream/kstream/topology"
)

type Predicate func(ctx context.Context, key interface{}, val interface{}) (bool, error)

type BranchDetails struct {
	Name      string
	Predicate Predicate
}

type BranchChild struct {
	id        topology.NodeId
	Name      string
	Predicate Predicate
	edges     []topology.Node
}

func (b *BranchChild) AddEdge(node topology.Node) {
	b.edges = append(b.edges, node)
}

func (b *BranchChild) Edges() []topology.Node {
	return b.edges
}

func (b *BranchChild) SetId(id topology.NodeId) {
	b.id = id
}

func (b *BranchChild) Id() topology.NodeId {
	return b.id
}

func (b *BranchChild) Build() (topology.Node, error) {
	return b, nil
}

func (b *BranchChild) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
	for _, child := range b.edges {
		_, _, next, err := child.Run(ctx, kIn, vIn)
		if err != nil || !next {
			return nil, nil, false, err
		}
	}

	return kIn, kOut, true, nil
}

func (b *BranchChild) Type() topology.Type {
	return topology.Type{
		Name:  "branchchild",
		Attrs: nil,
	}
}
