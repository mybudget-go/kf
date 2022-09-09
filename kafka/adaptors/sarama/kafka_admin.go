/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package sarama

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/pkg/errors"
	"github.com/tryfix/log"
	"time"
)

type adminOptions struct {
	BootstrapServers []string
	KafkaVersion     sarama.KafkaVersion
	Logger           log.Logger
}

func (opts *adminOptions) apply(options ...AdminOption) {
	opts.KafkaVersion = sarama.V2_4_0_0
	opts.Logger = log.NewNoopLogger()
	for _, opt := range options {
		opt(opts)
	}
}

type AdminOption func(*adminOptions)

func WithKafkaVersion(version sarama.KafkaVersion) AdminOption {
	return func(options *adminOptions) {
		options.KafkaVersion = version
	}
}

func WithLogger(logger log.Logger) AdminOption {
	return func(options *adminOptions) {
		options.Logger = logger
	}
}

type kAdmin struct {
	admin            sarama.ClusterAdmin
	logger           log.Logger
	tempTopicConfigs map[string]*kafka.Topic
}

func NewAdmin(bootstrapServer []string, options ...AdminOption) (*kAdmin, error) {
	opts := new(adminOptions)
	opts.apply(options...)
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = opts.KafkaVersion
	saramaConfig.Admin.Timeout = 20 * time.Second
	logger := opts.Logger.NewLog(log.Prefixed(`kafka-admin`))
	admin, err := sarama.NewClusterAdmin(bootstrapServer, saramaConfig)
	if err != nil {
		return nil, errors.Wrap(err, `admin client failed`)
	}

	return &kAdmin{
		admin:            admin,
		logger:           logger,
		tempTopicConfigs: map[string]*kafka.Topic{},
	}, nil
}

func (a *kAdmin) FetchInfo(topics []string) (map[string]*kafka.Topic, error) {
	if len(topics) < 1 {
		return nil, errors.New(`empty topic list`)
	}

	topicMeta, err := a.admin.DescribeTopics(topics)
	if err != nil {
		return nil, errors.Wrap(err, `cannot get metadata`)
	}

	topicInfo := make(map[string]*kafka.Topic)
	for _, tp := range topicMeta {
		var pts []kafka.PartitionConf
		var replica int
		for _, pt := range tp.Partitions {
			pts = append(pts, kafka.PartitionConf{
				Id:    pt.ID,
				Error: pt.Err,
			})
			replica = len(pt.Replicas)
		}
		topicInfo[tp.Name] = &kafka.Topic{
			Name:              tp.Name,
			Partitions:        pts,
			NumPartitions:     int32(len(pts)),
			ReplicationFactor: int16(replica),
		}
		if tp.Err != sarama.ErrNoError {
			topicInfo[tp.Name].Error = tp.Err
		}

		// configs
		confs, err := a.admin.DescribeConfig(sarama.ConfigResource{
			Type:        sarama.TopicResource,
			Name:        tp.Name,
			ConfigNames: []string{`cleanup.policy`, `min.insync.replicas`, `retention.ms`},
		})
		if err != nil {
			return nil, errors.Wrapf(err, `DescribeConfig failed for topic %s`, tp.Name)
		}
		topicInfo[tp.Name].ConfigEntries = map[string]string{}
		for _, co := range confs {
			topicInfo[tp.Name].ConfigEntries[co.Name] = co.Value
		}
	}

	return topicInfo, nil
}

func (a *kAdmin) ListTopics() ([]string, error) {
	topics, err := a.admin.ListTopics()
	if err != nil {
		return nil, errors.Wrap(err, `cannot get metadata`)
	}

	var tpList []string

	for tp := range topics {
		tpList = append(tpList, tp)
	}

	return tpList, nil
}

func (a *kAdmin) CreateTopics(topics []*kafka.Topic) error {
	var tpNames []string
	for _, info := range topics {
		tpNames = append(tpNames, info.Name)
		details := &sarama.TopicDetail{
			NumPartitions:     info.NumPartitions,
			ReplicationFactor: info.ReplicationFactor,
			ReplicaAssignment: info.ReplicaAssignment,
			ConfigEntries:     map[string]*string{},
		}

		for cName := range info.ConfigEntries {
			conf := info.ConfigEntries[cName]
			details.ConfigEntries[cName] = &conf
		}

		err := a.admin.CreateTopic(info.Name, details, false)
		if err != nil {
			if e, ok := err.(*sarama.TopicError); ok && (e.Err == sarama.ErrTopicAlreadyExists || e.Err == sarama.ErrNoError) {
				a.logger.Warn(err)
				continue
			}
			return errors.Wrapf(err, `could not create topic [%s]`, info.Name)
		}

		a.logger.Info(`k-stream.kafkaAdmin`,
			fmt.Sprintf(`kafkaAdmin topic [%s] created`, info.Name))
	}

	// After create request returns, it might take a couple of seconds for the broker to be aware of the creation.
	// So lets wait to verify that
	a.verifyAction(`CREATE`, tpNames)

	return nil
}

func (a *kAdmin) StoreConfigs(topics []*kafka.Topic) error {
	for _, topic := range topics {
		if _, ok := a.tempTopicConfigs[topic.Name]; ok {
			return errors.Errorf(`topic %s already marked for creation`, topic.Name)
		}

		a.tempTopicConfigs[topic.Name] = topic
	}

	return nil
}

func (a *kAdmin) ApplyConfigs() error {
	var topics []*kafka.Topic
	for _, tp := range a.tempTopicConfigs {
		topics = append(topics, tp)
	}

	if err := a.CreateTopics(topics); err != nil {
		return errors.Wrap(err, `config apply failed`)
	}

	return nil
}

func (a *kAdmin) DeleteTopics(topics []string) error {
	if len(topics) < 1 {
		return nil
	}

	for _, topic := range topics {
		err := a.admin.DeleteTopic(topic)
		if err != nil && !errors.Is(err, sarama.ErrUnknownTopicOrPartition) {
			return errors.Wrap(err, fmt.Sprintf(`could not delete topic [%s]`, topic))
		}
	}

	// After delete request returns, it might take a couple of seconds for the broker to be aware of the deletion.
	// So lets wait to verify that
	a.verifyAction(`DELETE`, topics)

	return nil
}

func (a *kAdmin) verifyAction(action string, topics []string) {
	a.logger.Warn(fmt.Sprintf(`Topics [%s] still in progress for topics %v. Waiting...`, action, topics))
	time.Sleep(1 * time.Second)
	tps, _ := a.FetchInfo(topics)

	switch action {
	case `delete`:
		// if fetch returns more than zero topics that means the deletion for some topics are still in progress
		if len(tps) > 0 {
			a.verifyAction(action, topics)
		}
	case `create`:
		if tps == nil || len(tps) != len(topics) {
			a.verifyAction(action, topics)
		}
	}
}

func (a *kAdmin) Close() {
	if err := a.admin.Close(); err != nil {
		a.logger.Warn(fmt.Sprintf(`kafkaAdmin cannot close broker : %+v`, err))
	}
}
