package streams

import (
	"context"
	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/streams/encoding"
	"github.com/gmbyapa/kstream/streams/tasks"
	"github.com/gmbyapa/kstream/streams/topology"
)

// SourceCtxParamExtractor extracts a key:val pair from a record. Used to bind key:val pairs into the record Context.
type SourceCtxParamExtractor func(record kafka.Record) (key string, val interface{})

// KSourceOption is used to customize the KSource
type KSourceOption func(*KSource)

// ConsumeWithContextParamExtractors adds a list of SourceCtxParamBinder func to the topology.Source
// and each binder will be called in a loop for each record
func ConsumeWithContextParamExtractors(fn ...SourceCtxParamExtractor) KSourceOption {
	return func(source *KSource) {
		source.sourceCtxParamBinders = fn
	}
}

type AutoCreateOptions struct {
	enabled       bool
	partitionedAs topology.Source
	replicas      int16
}

type TopicAutoCreateOption func(*AutoCreateOptions)

func AutoTopicCreateReplicaCount(replicas int16) TopicAutoCreateOption {
	return func(options *AutoCreateOptions) {
		options.replicas = replicas
	}
}

func AutoTopicCreatPartitionedAs(src topology.Source) TopicAutoCreateOption {
	return func(options *AutoCreateOptions) {
		options.partitionedAs = src
	}
}

func ConsumeWithAutoTopicCreateEnabled(options ...TopicAutoCreateOption) KSourceOption {
	return func(source *KSource) {
		source.autoCreateOptions.enabled = true
		for _, opt := range options {
			opt(source.autoCreateOptions)
		}
	}
}

func ConsumeWithTopicNameFormatterFunc(fn TopicNameFormatter) KSourceOption {
	return func(source *KSource) {
		source.topicNameFormatter = fn
	}
}

func ConsumeWithTaskOpts(opt ...tasks.TaskOpt) KSourceOption {
	return func(source *KSource) {
		source.taskOptions = append(source.taskOptions, opt...)
	}
}

func ConsumeWithKeyEncoder(encoder encoding.Encoder) KSourceOption {
	return func(source *KSource) {
		source.encoder.Key = encoder
	}
}

func ConsumeWithValEncoder(encoder encoding.Encoder) KSourceOption {
	return func(source *KSource) {
		source.encoder.Value = encoder
	}
}

func markAsInternal() KSourceOption {
	return func(source *KSource) {
		source.internal = true
	}
}

type KSource struct {
	encoder               topology.SourceEncoder
	topic                 string
	offsetReset           kafka.Offset
	coPartitionedWith     topology.Source
	sourceCtxParamBinders []SourceCtxParamExtractor
	autoCreateOptions     *AutoCreateOptions
	taskOptions           []tasks.TaskOpt
	internal              bool
	topicNameFormatter    TopicNameFormatter
	topology.DefaultNode
}

func NewKSource(topic string, opts ...KSourceOption) topology.Source {
	src := &KSource{
		topic:             topic,
		autoCreateOptions: new(AutoCreateOptions),
	}

	// apply options
	for _, opt := range opts {
		opt(src)
	}

	return src
}

func (s *KSource) Type() topology.Type {
	return topology.Type{
		Name: `kSource`,
		Attrs: map[string]string{
			`topic`: s.topic,
		},
	}
}

func (s *KSource) Build(ctx topology.SubTopologyContext) (topology.Node, error) {
	// validate mandatory options
	if s.encoder.Key == nil {
		return nil, s.Err(`key encoder not provided`)
	}

	if s.encoder.Value == nil {
		return nil, s.Err(`val encoder not provided`)
	}

	return &KSource{
		encoder:               s.encoder,
		topic:                 s.topic,
		offsetReset:           s.offsetReset,
		DefaultNode:           s.DefaultNode,
		sourceCtxParamBinders: s.sourceCtxParamBinders,
	}, nil
}

func (s *KSource) Setup(ctx topology.SubTopologySetupContext) error {
	if s.autoCreateOptions.enabled {
		if s.topicNameFormatter != nil {
			s.topic = s.topicNameFormatter(s.topic)(ctx, s.Id())
		}

		topic := &kafka.Topic{
			Name:              s.topic,
			NumPartitions:     ctx.MaxPartitionCount(),
			ReplicationFactor: s.autoCreateOptions.replicas,
		}

		// The topology only have auto create topics. looking for
		// autoCreateOptions.partitionedAs to get the number of
		// partitions form the parent
		// TODO what if topology has more than one auto create topics
		if ctx.MaxPartitionCount() < 1 && s.autoCreateOptions.partitionedAs != nil {
			topic.NumPartitions = ctx.TopicMeta()[s.autoCreateOptions.partitionedAs.Topic()].NumPartitions
		}

		if err := ctx.Admin().StoreConfigs([]*kafka.Topic{topic}); err != nil {
			return s.WrapErr(err)
		}
	}

	return nil
}

func (s *KSource) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
	k, err := s.encoder.Key.Decode(kIn.([]byte))
	if err != nil {
		return nil, nil, false, encoding.Err{Err: s.WrapErrWith(err, `key decode error`)}
	}

	v, err := s.encoder.Value.Decode(vIn.([]byte))
	if err != nil {
		return nil, nil, false, encoding.Err{Err: s.WrapErrWith(err, `value decode error`)}
	}

	if s.sourceCtxParamBinders != nil {
		for _, bind := range s.sourceCtxParamBinders {
			k, v := bind(topology.RecordFromContext(ctx))
			ctx = context.WithValue(ctx, k, v)
		}
	}

	return s.Forward(ctx, k, v, true)
}

func (s *KSource) Encoder() topology.SourceEncoder {
	return s.encoder
}

func (s *KSource) Topic() string {
	return s.topic
}

func (s *KSource) ShouldCoPartitionedWith(source topology.Source) {
	source.(*KSource).coPartitionedWith = s
	s.coPartitionedWith = source
}

func (s *KSource) CoPartitionedWith() topology.Source {
	return s.coPartitionedWith
}

func (s *KSource) RePartitionedAs() topology.Source {
	if s.autoCreateOptions.partitionedAs == nil {
		return nil
	}

	if s.autoCreateOptions.partitionedAs.AutoCreate() {
		return s.getExistingParent(s.autoCreateOptions.partitionedAs)
	}

	return s.autoCreateOptions.partitionedAs
}

func (s *KSource) getExistingParent(src topology.Source) topology.Source {
	if src != nil && src.AutoCreate() {
		return s.getExistingParent(src.RePartitionedAs())
	}

	return src
}

func (s *KSource) AutoCreate() bool {
	return s.autoCreateOptions.enabled
}

func (s *KSource) Internal() bool {
	return s.autoCreateOptions.enabled
}
