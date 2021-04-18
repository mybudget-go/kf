package streams

import (
	"github.com/tryfix/kstream/kstream/serdes"
	"github.com/tryfix/kstream/kstream/streams/dsl"
	"github.com/tryfix/kstream/kstream/topology"
)

type StreamBuilder struct {
	tpBuilder         topology.TopologyBuilder
	defaultStpBuilder topology.SubTopologyBuilder
	config            *Config
	defaultBuilders   *DefaultBuilders
}

func NewStreamBuilder(config *Config) *StreamBuilder {
	b := &StreamBuilder{
		tpBuilder: new(topology.KTopologyBuilder),
		//defaultStpBuilder: new(topology.KSubTopologyBuilder),
		config:          config,
		defaultBuilders: new(DefaultBuilders),
	}
	//b.tpBuilder.AddSubTopology(b.defaultStpBuilder)
	b.defaultBuilders.build(b.config)

	return b
}

func (b *StreamBuilder) KStream(topic string, keyEnc, valEnc serdes.Deserializer) dsl.Stream {
	// need to validate topics are co-partitioned or not
	// if not co-partitioned it has to be a new sub stpBuilder

	// multiple sub-topologies cannot be joined together without co-partitioning

	source := NewKSource(topic, topology.SourceDeserializer{
		Key:   keyEnc,
		Value: valEnc,
	})
	sTp := &topology.KSubTopologyBuilder{}
	b.tpBuilder.AddSubTopology(sTp)

	sTp.AddSource(source)
	//b.defaultStpBuilder.AddNode(source)

	return &kStream{
		tpBuilder:  b.tpBuilder,
		stpBuilder: sTp,
		node:       source,
		builder:    b,
	}
}

func (b *StreamBuilder) Build(streams ...dsl.StreamTopology) Runner {
	//for _, stream := range streams{
	//	b.tpBuilder.AddSubTopology(stream.Topology())
	//}

	tp, err := b.tpBuilder.Build()
	if err != nil {
		panic(err)
	}

	return &KRunner{
		topology:      tp,
		builder:       b,
		subTopologies: map[string]topology.SubTopology{},
	}
}
