package sarama

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/tryfix/kstream/consumer"
	"github.com/tryfix/kstream/data"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
	"time"
)

type topicPartitionClaim struct {
	topic     string
	partition int32
	messages  chan *data.Record
}

func (t *topicPartitionClaim) Topic() string {
	return t.topic
}

func (t *topicPartitionClaim) Partition() int32 {
	return t.partition
}

func (t *topicPartitionClaim) Messages() <-chan *data.Record {
	return t.messages
}

type groupHandler struct {
	handler               consumer.GroupHandler
	logger                log.Logger
	recordUuidExtractFunc consumer.RecordUuidExtractFunc
	metrics               struct {
		reporter         metrics.Reporter
		reBalancing      metrics.Gauge
		commitLatency    metrics.Observer
		reBalanceLatency metrics.Observer
		endToEndLatency  metrics.Observer
	}
}

func (g *groupHandler) Setup(session sarama.ConsumerGroupSession) error {
	gSession := &groupSession{
		tps:     g.extractTps(session.Claims()),
		session: session,
	}
	return g.handler.OnPartitionAssigned(session.Context(), gSession)
}

func (g *groupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	gSession := &groupSession{tps: g.extractTps(session.Claims())}
	return g.handler.OnPartitionRevoked(session.Context(), gSession)
}

func (g *groupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	gSession := &groupSession{tps: g.extractTps(session.Claims())}
	tpClaim := &topicPartitionClaim{
		topic:     claim.Topic(),
		partition: claim.Partition(),
		messages:  make(chan *data.Record, 100),
	}

	go func() {
	MLOOP:
		for {
			select {
			case <-session.Context().Done():
				break MLOOP

			case msg := <-claim.Messages():
				t := time.Since(msg.Timestamp)
				g.metrics.endToEndLatency.Observe(float64(t.Nanoseconds()/1e3), map[string]string{
					`topic`:     msg.Topic,
					`partition`: fmt.Sprint(msg.Partition),
				})

				record := &data.Record{
					Key:       msg.Key,
					Value:     msg.Value,
					Offset:    msg.Offset,
					Topic:     msg.Topic,
					Partition: msg.Partition,
					Timestamp: msg.Timestamp,
				}

				for _, h := range msg.Headers {
					record.Headers = append(record.Headers, data.RecordHeader{
						Key:   h.Key,
						Value: h.Value,
					})
				}

				uuid := g.recordUuidExtractFunc(record)
				record.UUID = uuid

				g.logger.Trace("record received after " +
					t.String() +
					" for " + tpClaim.Topic() +
					" with key: " + string(msg.Key) +
					" with record-id [" + record.UUID.String() + "]")

				tpClaim.messages <- record

			}
		}

		close(tpClaim.messages)
	}()

	return g.handler.Consume(session.Context(), gSession, tpClaim)
}

func (g *groupHandler) extractTps(kafkaTps map[string][]int32) []consumer.TopicPartition {
	tps := make([]consumer.TopicPartition, 0)
	for topic, partitions := range kafkaTps {
		for _, p := range partitions {
			tps = append(tps, consumer.TopicPartition{
				Topic:     topic,
				Partition: p,
			})
		}
	}
	return tps
}

func (g *groupHandler) setUpMetrics() {
	g.metrics.commitLatency = g.metrics.reporter.Observer(metrics.MetricConf{
		Path:        `k_stream_consumer_commit_latency_microseconds`,
		ConstLabels: map[string]string{`group`: `group`},
	})
	g.metrics.endToEndLatency = g.metrics.reporter.Observer(metrics.MetricConf{
		Path:        `k_stream_consumer_end_to_latency_latency_microseconds`,
		Labels:      []string{`topic`, `partition`},
		ConstLabels: map[string]string{`group`: `group`},
	})
	g.metrics.reBalanceLatency = g.metrics.reporter.Observer(metrics.MetricConf{
		Path:        `k_stream_consumer_re_balance_latency_microseconds`,
		ConstLabels: map[string]string{`group`: `group`},
	})
	g.metrics.reBalancing = g.metrics.reporter.Gauge(metrics.MetricConf{
		Path:        `k_stream_consumer_rebalancing`,
		ConstLabels: map[string]string{`group`: `group`},
	})
	g.metrics.reBalancing = g.metrics.reporter.Gauge(metrics.MetricConf{
		Path:        `k_stream_consumer_rebalancing_implement`,
		ConstLabels: map[string]string{`group`: `group`},
	})
}

func (g *groupHandler) cleanUpMetrics() {
	g.metrics.commitLatency.UnRegister()
	g.metrics.endToEndLatency.UnRegister()
	g.metrics.reBalanceLatency.UnRegister()
	g.metrics.reBalancing.UnRegister()
}
