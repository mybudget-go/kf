package processors

import (
	"context"
	"github.com/tryfix/errors"
	"github.com/tryfix/kstream/kstream/topology"
)

type FilterFunc func(ctx context.Context, key, value interface{}) (bool, error)

type Filter struct {
	id         topology.NodeId
	FilterFunc FilterFunc
	edges      []topology.Node
}

func (f *Filter) Id() topology.NodeId {
	return f.id
}

func (f *Filter) SetId(id topology.NodeId) {
	f.id = id
}

func (f *Filter) AddEdge(node topology.Node) {
	f.edges = append(f.edges, node)
}

func (f *Filter) Name() string {
	return `filter`
}

func (f *Filter) Type() topology.Type {
	return topology.Type{
		Name:  "filter",
		Attrs: nil,
	}
}

func (f *Filter) Edges() []topology.Node {
	return f.edges
}

func (f *Filter) Build() (topology.Node, error) {
	return &Filter{
		FilterFunc: f.FilterFunc,
		id:         f.Id(),
	}, nil
}

func (f *Filter) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, next bool, err error) {
	ok, err := f.FilterFunc(ctx, kIn, vIn)
	if err != nil {
		return nil, nil, false, errors.WithPrevious(err, `process error`)
	}

	if !ok {
		return nil, nil, false, nil
	}

	for _, child := range f.edges {
		_, _, next, err := child.Run(ctx, kIn, vIn)
		if err != nil || !next {
			return nil, nil, false, err
		}
	}

	return kIn, vIn, ok, nil
}
