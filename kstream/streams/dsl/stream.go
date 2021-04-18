package dsl

import (
	"github.com/tryfix/kstream/kstream/processors"
	"github.com/tryfix/kstream/kstream/serdes"
	"github.com/tryfix/kstream/kstream/topology"
)

type StreamTopology interface {
	Topology() topology.SubTopologyBuilder
}

type SinkOption interface{}

type Stream interface {
	Branch(branches ...processors.BranchDetails) []Stream
	//SelectKey(selectKeyFunc processors.SelectKeyFunc) Stream
	//TransformValue(valTransformFunc processors.ValueTransformFunc) Stream
	//Transform(transformer processors.TransFunc) Stream
	Filter(filter processors.FilterFunc) Stream
	//Process(processor processors.ProcessFunc) Stream
	//JoinGlobalTable(table Stream, keyMapper join.KeyMapper, valMapper join.ValueMapper, typ join.Type) Stream
	//JoinKTable(stream Stream, keyMapper join.KeyMapper, valMapper join.ValueMapper, typ join.Type) Stream
	//JoinStream(stream Stream, valMapper join.ValueMapper,typ join.Type, opts ...join.RepartitionOption) Stream
	Through(topic string, keySerDe, valSerDe serdes.SerDes, options ...SinkOption) Stream
	Merge(stream Stream) Stream
	//Materialize(topic, storeName string, keyEncoder encoding.Builder, valEncoder encoding.Builder, options ...processors.MaterializeOption) Stream
	To(topic string, keySerializer, valSerializer serdes.Serializer, options ...SinkOption)
	StreamTopology
}
