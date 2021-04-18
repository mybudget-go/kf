package topology

import (
	"context"
)

type SubTopology interface {
	Id() SubTopologyId
	Sources() []Source
	Sinks() []Sink
	Run(ctx context.Context, kIn, vIn interface{}) error
	Root() Node
	Edges() []Edge
	Nodes() []Node
}

type KSubTopology struct {
	root    Node
	id      SubTopologyId
	edges   []Edge
	nodes   []Node
	sources []Source
	sinks   []Sink
}

func (t *KSubTopology) Sources() []Source {
	return t.sources
}

func (t *KSubTopology) Sinks() []Sink {
	return t.sinks
}

func (t *KSubTopology) Id() SubTopologyId {
	return t.id
}

func (t *KSubTopology) Root() Node {
	return t.root
}

func (t *KSubTopology) Edges() []Edge {
	return t.edges
}

func (t *KSubTopology) Nodes() []Node {
	return t.nodes
}

func (t *KSubTopology) Run(ctx context.Context, kIn, vIn interface{}) error {
	// if topology has sinks start a transaction
	if _, _, _, err := t.root.Run(ctx, kIn, vIn); err != nil {
		// abort the transaction
		return err
	}

	return nil
}
