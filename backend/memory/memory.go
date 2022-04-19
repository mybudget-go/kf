/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package memory

import (
	"github.com/gmbyapa/kstream/backend"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
	"sync"
	"time"
)

type memoryRecord struct {
	key       []byte
	value     []byte
	createdAt time.Time
	expiry    time.Duration
}

type config struct {
	RecordExpiryEnabled          bool
	RecordExpiry                 time.Duration
	ExpiredRecordCleanupInterval time.Duration
	MetricsReporter              metrics.Reporter
}

func NewConfig() *config {
	conf := new(config)
	conf.parse()

	return conf
}

func (c *config) parse() {
	if c.ExpiredRecordCleanupInterval == time.Duration(0) {
		c.ExpiredRecordCleanupInterval = 10 * time.Second
	}

	if c.MetricsReporter == nil {
		c.MetricsReporter = metrics.NoopReporter()
	}
}

type memory struct {
	name                         string
	expiredRecordCleanupInterval time.Duration
	globalRecordExpiry           time.Duration
	recordExpiryEnabled          bool
	records                      *sync.Map
	logger                       log.Logger
	metrics                      struct {
		readLatency   metrics.Observer
		updateLatency metrics.Observer
		deleteLatency metrics.Observer
		storageSize   metrics.Gauge
	}
}

func Builder(config *config) backend.Builder {
	return func(name string) (backend backend.Backend, err error) {
		return NewMemoryBackend(name, config), nil
	}
}

func NewMemoryBackend(name string, config *config) backend.Backend {
	m := &memory{
		name:                         name,
		globalRecordExpiry:           config.RecordExpiry,
		recordExpiryEnabled:          config.RecordExpiryEnabled,
		expiredRecordCleanupInterval: config.ExpiredRecordCleanupInterval,
		records:                      new(sync.Map),
	}

	labels := []string{`name`, `type`}
	m.metrics.readLatency = config.MetricsReporter.Observer(metrics.MetricConf{Path: `backend_read_latency_microseconds`, Labels: labels})
	m.metrics.updateLatency = config.MetricsReporter.Observer(metrics.MetricConf{Path: `backend_update_latency_microseconds`, Labels: labels})
	m.metrics.storageSize = config.MetricsReporter.Gauge(metrics.MetricConf{Path: `backend_storage_size`, Labels: labels})
	m.metrics.deleteLatency = config.MetricsReporter.Observer(metrics.MetricConf{Path: `backend_delete_latency_microseconds`, Labels: labels})

	if m.recordExpiryEnabled {
		go m.runCleaner()
	}

	return m
}

func (m *memory) runCleaner() {
	ticker := time.NewTicker(m.expiredRecordCleanupInterval)
	for range ticker.C {
		records := m.snapshot()
		for _, record := range records {
			age := time.Since(record.createdAt).Nanoseconds()
			if (record.expiry > 0 && age > record.expiry.Nanoseconds()) ||
				m.globalRecordExpiry > 0 && age > m.globalRecordExpiry.Nanoseconds() {
				if err := m.Delete(record.key); err != nil {
					m.logger.Error(err)
				}
			}
		}
	}
}

func (m *memory) snapshot() []memoryRecord {
	records := make([]memoryRecord, 0)

	m.records.Range(func(key, value interface{}) bool {
		records = append(records, value.(memoryRecord))
		return true
	})

	return records
}

func (m *memory) Name() string {
	return m.name
}

func (m *memory) String() string {
	return `memory`
}

func (m *memory) Persistent() bool {
	return false
}

func (m *memory) Set(key []byte, value []byte, expiry time.Duration) error {
	defer func(begin time.Time) {
		m.metrics.updateLatency.Observe(float64(time.Since(begin).Nanoseconds()/1e3), map[string]string{`name`: m.Name(), `type`: `memory`})
	}(time.Now())

	record := memoryRecord{
		key:       key,
		value:     value,
		expiry:    expiry,
		createdAt: time.Now(),
	}

	m.records.Store(string(key), record)

	return nil
}

func (m *memory) Get(key []byte) ([]byte, error) {
	defer func(begin time.Time) {
		m.metrics.readLatency.Observe(float64(time.Since(begin).Nanoseconds()/1e3), map[string]string{`name`: m.Name(), `type`: `memory`})
	}(time.Now())

	record, ok := m.records.Load(string(key))
	if !ok {
		return nil, nil
	}

	return record.(memoryRecord).value, nil
}

func (m *memory) RangeIterator(fromKy []byte, toKey []byte) backend.Iterator {
	panic("implement me")
}

func (m *memory) Iterator() backend.Iterator {
	records := m.snapshot()
	return &Iterator{
		records: records,
		valid:   len(records) > 0,
	}
}

func (m *memory) Delete(key []byte) error {

	defer func(begin time.Time) {
		m.metrics.deleteLatency.Observe(float64(time.Since(begin).Nanoseconds()/1e3), map[string]string{`name`: m.Name(), `type`: `memory`})
	}(time.Now())

	m.records.Delete(string(key))

	return nil
}

func (m *memory) Destroy() error { return nil }

func (m *memory) SetExpiry(time time.Duration) {}

func (m *memory) Close() error {
	m.records = nil
	return nil
}
