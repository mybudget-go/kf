package streams

import (
	"github.com/tryfix/kstream/kstream/processors"
	"github.com/tryfix/kstream/kstream/serdes"
	"github.com/tryfix/kstream/kstream/streams/dsl"
	"github.com/tryfix/kstream/kstream/topology"
)

type kStream struct {
	tpBuilder  topology.TopologyBuilder
	stpBuilder topology.SubTopologyBuilder
	node       topology.NodeBuilder
	builder    *StreamBuilder
}

func (k *kStream) childStream(node topology.NodeBuilder) *kStream {
	return &kStream{
		tpBuilder:  k.tpBuilder,
		stpBuilder: k.stpBuilder,
		node:       node,
		builder:    k.builder,
	}
}

func (k *kStream) Branch(branches ...processors.BranchDetails) []dsl.Stream {
	// create a branch node
	br := new(processors.Branch)
	k.stpBuilder.AddNodeWithEdge(k.node, br)

	var childStreams []dsl.Stream

	for _, child := range branches {
		childNode := &processors.BranchChild{
			Name:      child.Name,
			Predicate: child.Predicate,
		}
		k.stpBuilder.AddNodeWithEdge(br, childNode)
		childStreams = append(childStreams, k.childStream(childNode))
	}

	return childStreams
}

/*// Note: Applying Joins or materialize states after this operator must provide repartition options
func (k *kStream) SelectKey(selectKeyFunc processors.SelectKeyFunc) dsl.Stream {
	panic("implement me")
}

func (k *kStream) TransformValue(valTransformFunc processors.ValueTransformFunc) dsl.Stream {
	panic("implement me")
}

// Note: Applying Joins or materialize states after this operator must provide repartition options
func (k *kStream) Transform(transformer processors.TransFunc) dsl.Stream {
	panic("implement me")
}*/

func (k *kStream) Filter(filter processors.FilterFunc) dsl.Stream {
	node := &processors.Filter{
		FilterFunc: filter,
	}
	k.stpBuilder.AddNodeWithEdge(k.node, node)

	return k.childStream(node)
}

//func (k *kStream) Process(processor processors.ProcessFunc) dsl.Stream {
//	panic("implement me")
//}

//func (k *kStream) JoinStream(stream dsl.Stream, valMapper join.ValueMapper, typ join.Type, opts ...join.RepartitionOption) dsl.Stream {
//	panic("implement me")
//}

//func (k *kStream) JoinGlobalTable(table kstream.GlobalTable, keyMapper join.KeyMapper, valMapper join.ValueMapper, typ join.Type) Stream{
//
//	joiner := &join.GlobalTableJoiner{
//		Typ:         typ,
//		Store:       joinStream.storeName,
//		KeyMapper:   keyMapper,
//		ValueMapper: valMapper,
//		NId:          s.NodeBuilder.Id().NewNode(``),
//	}
//
//	s.Node.AddChild(joiner)
//	s.NodeBuilder.AddChildBuilder(joiner)
//
//	joined := newKStream(nil, nil, nil, s, s.rootStream.config)
//	joined.Node = joiner
//	joined.NodeBuilder = joiner
//	joined.rootStream = s.rootStream
//	joined.keySelected = s.keySelected
//}

func (k *kStream) Through(topic string, keySerDe, valSerDe serdes.SerDes, options ...dsl.SinkOption) dsl.Stream {
	// create a sink
	opts := []dsl.SinkOption{
		SinkWithProducerBuilder(k.builder.defaultBuilders.Producer),
		SinkWithLogger(k.builder.config.Logger),
	}
	sink := NewKSink(KSinkConfigs{
		Topic: topic,
		Serializers: topology.SinkSerializer{
			Key:   keySerDe,
			Value: valSerDe,
		},
	}, append(options, opts...)...)
	k.stpBuilder.AddNodeWithEdge(k.node, sink)

	// create a new sub stpBuilder
	tp := &topology.KSubTopologyBuilder{}
	source := NewKSource(topic, topology.SourceDeserializer{
		Key:   keySerDe,
		Value: valSerDe,
	})
	tp.AddSource(source)
	tp.AddNode(source)

	k.tpBuilder.AddSubTopology(tp)

	return &kStream{
		tpBuilder:  k.tpBuilder,
		stpBuilder: tp,
		node:       source,
	}
}

func (k *kStream) Merge(stream dsl.Stream) dsl.Stream {
	// merging with
	panic("implement me")
}

func (k *kStream) To(topic string, keySerDe, valSerDe serdes.Serializer, options ...dsl.SinkOption) {

	opts := []dsl.SinkOption{
		SinkWithProducerBuilder(k.builder.defaultBuilders.Producer),
		SinkWithLogger(k.builder.config.Logger),
	}
	sink := NewKSink(KSinkConfigs{
		Topic: topic,
		Serializers: topology.SinkSerializer{
			Key:   keySerDe,
			Value: valSerDe,
		},
	}, append(opts, options...)...)
	k.stpBuilder.AddNodeWithEdge(k.node, sink)
}

func (k *kStream) Topology() topology.SubTopologyBuilder {
	return k.stpBuilder
}
