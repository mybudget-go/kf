package streams

import (
	"context"
	"github.com/tryfix/errors"
	"github.com/tryfix/kstream/kstream/topology"
)

type KSource struct {
	id           topology.NodeId
	edges        []topology.Node
	deserializer topology.SourceDeserializer
	topic        string
}

func NewKSource(topic string, deserializer topology.SourceDeserializer) topology.Source {
	return &KSource{deserializer: deserializer, topic: topic}
}

func (s *KSource) Id() topology.NodeId {
	return s.id
}

func (s *KSource) SetId(id topology.NodeId) {
	s.id = id
}

func (s *KSource) Type() topology.Type {
	return topology.Type{
		Name: "source",
		Attrs: map[string]string{
			`topic`: s.topic,
		},
	}
}

func (s *KSource) Build() (topology.Node, error) {
	return s, nil
}

func (s *KSource) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
	k, err := s.deserializer.Key.Deserialize(kIn.([]byte))
	if err != nil {
		return nil, nil, false, errors.WithPrevious(err, `key decode error`)
	}

	v, err := s.deserializer.Value.Deserialize(vIn.([]byte))
	if err != nil {
		return nil, nil, true, errors.WithPrevious(err, `value decode error`)
	}

	for _, child := range s.edges {
		_, _, next, err := child.Run(ctx, k, v)
		if err != nil {
			return nil, nil, false, err
		}

		if !next {
			return nil, nil, false, nil
		}
	}

	return nil, nil, false, nil
}

func (s *KSource) AddEdge(node topology.Node) {
	s.edges = append(s.edges, node)
}

func (s *KSource) Edges() []topology.Node {
	return s.edges
}

func (s *KSource) Deserializer() topology.SourceDeserializer {
	return s.deserializer
}

func (s *KSource) Topic() string {
	return s.topic
}
