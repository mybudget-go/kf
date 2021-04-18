package streams

import (
	"context"
	"fmt"
	"github.com/tryfix/errors"
	"github.com/tryfix/kstream/data"
	kContext "github.com/tryfix/kstream/kstream/context"
	"github.com/tryfix/kstream/kstream/serdes"
	"github.com/tryfix/kstream/kstream/streams/dsl"
	"github.com/tryfix/kstream/kstream/topology"
	"github.com/tryfix/kstream/producer"
	"github.com/tryfix/log"
	"time"
)

// TODO need to use more specific method
type SinkRecord struct {
	Key, Value interface{}
	Timestamp  time.Time          // only set if kafka is version 0.10+, inner message timestamp
	Headers    data.RecordHeaders // only set if kafka is version 0.11+
}

type TopicOverrider func(topic string) (overridden string)

type KSinkOption func(sink *kSink)

func (s *kSink) applyOptions(options ...KSinkOption) {
	for _, option := range options {
		option(s)
	}
}

func SinkWithProducerBuilder(p producer.Builder) KSinkOption {
	return func(sink *kSink) {
		sink.producerBuilder = p
	}
}

func SinkWithKeySerde(enc serdes.SerDes) KSinkOption {
	return func(sink *kSink) {
		sink.serializers.Key = enc
	}
}

func SinkWithValSerde(enc serdes.SerDes) KSinkOption {
	return func(sink *kSink) {
		sink.serializers.Value = enc
	}
}

func SinkWithRecordHeaderExtractor(f func(ctx context.Context, in SinkRecord) (headers data.RecordHeaders, err error)) KSinkOption {
	return func(sink *kSink) {
		sink.recordHeaderExtractor = f
	}
}

func SinkWithTombstoneFilter(f func(ctx context.Context, in SinkRecord) (tombstone bool)) KSinkOption {
	return func(sink *kSink) {
		sink.tombstoneFiler = f
	}
}

func SinkWithTopicOverrider(overrider TopicOverrider) KSinkOption {
	return func(sink *kSink) {
		sink.topicOverrider = overrider
	}
}

func SinkWithLogger(logger log.Logger) KSinkOption {
	return func(sink *kSink) {
		sink.logger = logger
	}
}

type kSink struct {
	id             topology.NodeId
	edges          []topology.Node
	serializers    topology.SinkSerializer
	topicOverrider TopicOverrider
	topic          string

	producerBuilder producer.Builder

	producers map[string]producer.Producer

	logger                log.Logger
	recordTransformer     func(ctx context.Context, in SinkRecord) (out SinkRecord, err error)
	recordHeaderExtractor func(ctx context.Context, in SinkRecord) (data.RecordHeaders, error)
	tombstoneFiler        func(ctx context.Context, in SinkRecord) (tombstone bool)
}

type KSinkConfigs struct {
	Topic       string
	Serializers topology.SinkSerializer
}

func NewKSink(configs KSinkConfigs, options ...dsl.SinkOption) topology.Sink {
	sink := &kSink{
		serializers: configs.Serializers,

		// apply default options
		topicOverrider: func(topic string) (overridden string) {
			return topic
		},
		tombstoneFiler: func(ctx context.Context, in SinkRecord) (tombstone bool) {
			return false
		},
		recordHeaderExtractor: func(ctx context.Context, in SinkRecord) (out data.RecordHeaders, err error) {
			return in.Headers, nil
		},
		recordTransformer: func(ctx context.Context, in SinkRecord) (out SinkRecord, err error) {
			return in, nil
		},
		producers: map[string]producer.Producer{},
	}

	var sinkOpts []KSinkOption
	for _, opt := range options {
		kOpt, ok := opt.(KSinkOption)
		if !ok {
			panic(`KSink: invalid option type`)
		}
		sinkOpts = append(sinkOpts, kOpt)
	}
	sink.applyOptions(sinkOpts...)

	// apply topic overrider
	sink.topic = sink.topicOverrider(configs.Topic)

	return sink
}

func (s *kSink) Id() topology.NodeId {
	return s.id
}

func (s *kSink) SetId(id topology.NodeId) {
	s.id = id
}

func (s *kSink) Type() topology.Type {
	return topology.Type{
		Name: "sink",
		Attrs: map[string]string{
			`topic`: s.topic,
		},
	}
}

func (s *kSink) Build() (topology.Node, error) {
	return s, nil
}

func (s *kSink) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
	record := new(data.Record)
	record.Timestamp = time.Now()
	record.Topic = s.topic

	skinRecord := SinkRecord{
		Key:       kIn,
		Value:     vIn,
		Timestamp: record.Timestamp,
		Headers:   kContext.Meta(ctx).Headers.All(),
	}

	// Deprecated: apply custom record transformations
	customRecord, err := s.recordTransformer(ctx, skinRecord)
	if err != nil {
		return nil, nil, false, err
	}
	skinRecord.Key = customRecord.Key
	skinRecord.Value = customRecord.Value
	skinRecord.Headers = customRecord.Headers
	skinRecord.Timestamp = customRecord.Timestamp

	// apply data record headers
	headers, err := s.recordHeaderExtractor(ctx, skinRecord)
	if err != nil {
		return nil, nil, false, errors.WithPrevious(err, `record extract failed`)
	}
	skinRecord.Headers = headers

	keyByt, err := s.serializers.Key.Serialize(skinRecord.Key)
	if err != nil {
		return nil, nil, false, errors.WithPrevious(err, `sink key encode error`)
	}
	record.Key = keyByt

	// if the record value is null or marked as null with a tombstoneFiler set the record value as null
	tombstoned := s.tombstoneFiler(ctx, skinRecord)
	if skinRecord.Key == nil || tombstoned {
		record.Value = nil
	} else {
		valByt, err := s.serializers.Value.Serialize(skinRecord.Value)
		if err != nil {
			return nil, nil, false, errors.WithPrevious(err, `sink value encode error`)
		}
		record.Value = valByt
	}

	record.Headers = skinRecord.Headers

	if _, _, err := s.producer(ctx).Produce(ctx, record); err != nil {
		return nil, nil, false, err
	}

	return nil, nil, true, nil
}

func (s *kSink) AddEdge(node topology.Node) {
	panic(`sink does not support edges`)
}

func (s *kSink) Edges() []topology.Node {
	return s.edges
}

func (s *kSink) Serializer() topology.SinkSerializer {
	return s.serializers
}

func (s *kSink) Close() error {
	for _, p := range s.producers {
		if err := p.Close(); err != nil {
			s.logger.Error(fmt.Sprintf(`Sink: cannot close producer due to %s`, err))
		}
	}

	return nil
}

func (s *kSink) AddProducer(topic string, partition int32) error {
	prId := fmt.Sprintf(`%s-%d`, topic, partition)
	if _, ok := s.producers[prId]; ok {
		return errors.New(`producer already exists`)
	}

	p, err := s.producerBuilder(func(config *producer.Config) {
		config.Transactional.Id = prId
	})
	if err != nil {
		return err
	}

	s.producers[prId] = p

	return nil
}

func (s *kSink) RemoveProducer(topic string, partition int32) error {
	prId := fmt.Sprintf(`%s-%d`, topic, partition)
	p, ok := s.producers[prId]
	if !ok {
		return errors.New(`producer does not exists`)
	}

	if err := p.Close(); err != nil {
		return err
	}

	delete(s.producers, prId)

	return nil
}

func (s *kSink) producer(ctx context.Context) producer.Producer {
	meta := kContext.Meta(ctx)
	return s.producers[fmt.Sprintf(`%s-%d`, meta.Topic, meta.Partition)]
}
