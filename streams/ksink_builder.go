package streams

import (
	"context"
	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/streams/encoding"
	"github.com/gmbyapa/kstream/streams/topology"
	"github.com/tryfix/log"
)

type KSinkOption func(sink *kSinkBuilder)

type Partitioner func(ctx context.Context, key, val interface{}, numPartitions int32) (int32, error)

func (s *kSinkBuilder) applyOptions(options ...KSinkOption) {
	for _, option := range options {
		option(s)
	}
}

func ProduceWithPartitioner(partitioner Partitioner) KSinkOption {
	return func(sink *kSinkBuilder) {
		sink.partitioner = partitioner
	}
}

func ProduceWithHeadersExtractor(h HeaderExtractor) KSinkOption {
	return func(sink *kSinkBuilder) {
		sink.headerExtractor = h
	}
}

func ProduceWithTombstoneFilter(f Tombstoner) KSinkOption {
	return func(sink *kSinkBuilder) {
		sink.tombstoneFiler = f
	}
}

func ProducerWithTopicNameFormatter(formatter TopicNameFormatter) KSinkOption {
	return func(sink *kSinkBuilder) {
		sink.topicNameFormatter = formatter
	}
}

func ProducerWithKeyEncoder(encoder encoding.Encoder) KSinkOption {
	return func(sink *kSinkBuilder) {
		sink.encoder.Key = encoder
	}
}

func ProducerWithValEncoder(encoder encoding.Encoder) KSinkOption {
	return func(sink *kSinkBuilder) {
		sink.encoder.Value = encoder
	}
}

func ProducerWithLogger(logger log.Logger) KSinkOption {
	return func(sink *kSinkBuilder) {
		sink.logger = logger
	}
}

type kSinkBuilder struct {
	topology.DefaultNode
	encoder            topology.SinkEncoder
	topicNameFormatter TopicNameFormatter
	topic              struct {
		name          string
		numPartitions int32
	}
	partitioner Partitioner

	logger log.Logger

	headerExtractor HeaderExtractor
	tombstoneFiler  Tombstoner
}

type KSinkBuilderConfigs struct {
	Topic   string
	Encoder topology.SinkEncoder
}

func NewKSinkBuilder(topic string, options ...KSinkOption) topology.NodeBuilder {
	sink := &kSinkBuilder{
		// apply default options
		tombstoneFiler: func(ctx context.Context, key, val interface{}) (tombstone bool) {
			return false
		},
		headerExtractor: func(ctx context.Context, key, val interface{}) kafka.RecordHeaders {
			return topology.RecordFromContext(ctx).Headers()
		},
	}

	sink.topic.name = topic

	sink.applyOptions(options...)

	return sink
}

func (s *kSinkBuilder) Topic() string {
	return s.topic.name
}

func (s *kSinkBuilder) Type() topology.Type {
	partitioner := `default`
	if s.partitioner != nil {
		partitioner = `custom`
	}
	return topology.Type{
		Name: "sink",
		Attrs: map[string]string{
			`topic`:       s.topic.name,
			`partitioner`: partitioner,
		},
	}
}

func (s *kSinkBuilder) Setup(ctx topology.SubTopologySetupContext) error {
	if s.topicNameFormatter != nil {
		s.topic.name = s.topicNameFormatter(s.topic.name)(ctx, s.Id())
	}

	return nil
}

func (s *kSinkBuilder) Build(ctx topology.SubTopologyContext) (topology.Node, error) {
	sink := &kSink{
		encoders:        s.encoder,
		partitioner:     s.partitioner,
		producer:        ctx.Producer(),
		logger:          s.logger,
		tombstoner:      s.tombstoneFiler,
		headerExtractor: s.headerExtractor,
		DefaultNode:     s.DefaultNode,
	}

	sink.topic.name = s.topic.name
	sink.topic.numOfPartitions = ctx.TopicMeta()[s.topic.name].NumPartitions

	return sink, nil
}

func (s *kSinkBuilder) AddEdge(node topology.Node) {
	panic(`sink does not support edges`)
}
