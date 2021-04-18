package processors

import (
	"context"
	"github.com/tryfix/errors"
	"github.com/tryfix/kstream/kstream/topology"
)

type Branch struct {
	id    topology.NodeId
	edges []topology.Node // NOTE: topology builder uses only
}

func (b *Branch) SetId(id topology.NodeId) {
	b.id = id
}

func (b *Branch) AddEdge(node topology.Node) {
	b.edges = append(b.edges, node)
}

func (b *Branch) Edges() []topology.Node {
	return b.edges
}

func (b *Branch) Id() topology.NodeId {
	return b.id
}

func (b *Branch) Build() (topology.Node, error) {
	return &Branch{
		id: b.id,
	}, nil
}

func (b *Branch) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
	for _, b := range b.edges {
		branch, _ := b.(*BranchChild)

		ok, err := branch.Predicate(ctx, kIn, vIn)
		if err != nil {
			return nil, nil, false, errors.WithPrevious(err, `predicate error`)
		}

		if ok {
			_, _, next, err := branch.Run(ctx, kIn, vIn)
			if err != nil || !next {
				return nil, nil, false, err
			}
			break
		}
	}

	return kIn, kOut, true, nil
}

func (b *Branch) Type() topology.Type {
	return topology.Type{
		Name:  "branch",
		Attrs: nil,
	}
}
