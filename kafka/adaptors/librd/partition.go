package librd

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	kafka2 "github.com/gmbyapa/kstream/kafka"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
	"sync"
)

type partition struct {
	//id consumer.TopicPartition
	events          chan kafka2.Event
	consumerErrors  chan error
	consumer        *kafka.Consumer
	logger          log.Logger
	metricsReporter metrics.Reporter
	//conf partitionConf
	metrics struct {
		//consumerBuffer    metrics.Gauge
		//consumerBufferMax metrics.Gauge
		endToEndLatency metrics.Observer
	}
	topic                        string
	partition                    int32
	partitionStart, partitionEnd int64
	closed                       bool
	mu                           *sync.Mutex
	//closing, closed chan struct{}
}

func (p *partition) init() {
	// setup metrics
	//labels := []string{`topic`, `partition`}
	p.metrics.endToEndLatency = p.metricsReporter.Observer(metrics.MetricConf{
		Path: `end_to_end_latency_microseconds`,
		//Labels: labels,
	})

	//go p.runBufferMetrics()
}

func (p *partition) Events() <-chan kafka2.Event {
	return p.events
}

func (p *partition) BeginOffset() kafka2.Offset {
	return kafka2.Offset(p.partitionStart)
}

func (p *partition) EndOffset() kafka2.Offset {
	return kafka2.Offset(p.partitionEnd)
}

//func (p *partition) runBufferMetrics() {
//	ticker := time.NewTicker(1 * time.Second)
//	for range ticker.C {
//		p.metrics.consumerBuffer.Count(float64(len(p.events)), map[string]string{
//			`topic`:     ``,
//			`partition`: `0`,
//			`type`:      `k_stream`,
//		})
//
//		p.metrics.consumerBufferMax.Count(float64(cap(p.events)), map[string]string{
//			`topic`:     ``,
//			`partition`: `0`,
//			`type`:      `k_stream`,
//		})
//	}
//}

func (p *partition) Close() error {
	p.logger.Info(`Partition closing...`)

	if err := p.consumer.IncrementalUnassign([]kafka.TopicPartition{{
		Topic:     &p.topic,
		Partition: p.partition,
	}}); err != nil {
		p.logger.Error(fmt.Sprintf("partition consumer [%s] failed to close", err))
	}

	//p.logger.Info(`Partition closing aq lock...`)
	//p.mu.Lock()
	//p.closed = true
	//p.mu.Unlock()
	//p.logger.Info(`Partition closing aq lock done...`)

	// TODO fix this
	//close(p.events)

	p.cleanUpMetrics()

	p.logger.Info(`partition closed`)

	return nil
}

func (p *partition) Send(record kafka2.Event) {
	//p.mu.Lock()
	//defer p.mu.Unlock()

	//if p.closed{
	//	return
	//}

	p.events <- record
}

func (p *partition) cleanUpMetrics() {
	//p.metrics.consumerBuffer.UnRegister()
	//p.metrics.consumerBufferMax.UnRegister()
	p.metrics.endToEndLatency.UnRegister()
}
