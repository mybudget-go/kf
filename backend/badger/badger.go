/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package badger

import (
	"fmt"
	badgerDB "github.com/dgraph-io/badger/v3"
	"github.com/gmbyapa/kstream/backend"
	"github.com/gmbyapa/kstream/pkg/errors"
	"github.com/tryfix/metrics"
	"time"
)

type ByteRecord struct {
	Key       []byte
	Value     []byte
	createdAt time.Time
	expiry    time.Duration
}

type config struct {
	StorageDir                   string
	InMemory                     bool
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
		c.ExpiredRecordCleanupInterval = 1 * time.Second
	}

	if c.StorageDir == `` {
		c.StorageDir = `storage`
	}

	if c.MetricsReporter == nil {
		c.MetricsReporter = metrics.NoopReporter()
	}
}

type badger struct {
	name                         string
	db                           *badgerDB.DB
	expiredRecordCleanupInterval time.Duration
	globalRecordExpiry           time.Duration
	metrics                      struct {
		readLatency   metrics.Observer
		updateLatency metrics.Observer
		deleteLatency metrics.Observer
		storageSize   metrics.Gauge
	}
}

func Builder(config *config) backend.Builder {
	return func(name string) (backend backend.Backend, err error) {
		return NewBadgerBackend(name, config), nil
	}
}

func NewBadgerBackend(name string, config *config) backend.Backend {
	storageDir := fmt.Sprintf(`%s/backends/badger/%s`, config.StorageDir, name)
	if config.InMemory {
		storageDir = ``
	}
	db, err := badgerDB.
		Open(badgerDB.DefaultOptions(storageDir).
			WithLoggingLevel(badgerDB.ERROR).
			WithInMemory(config.InMemory))
	if err != nil {
		panic(err)
	}

	m := &badger{
		name:                         name,
		db:                           db,
		expiredRecordCleanupInterval: config.ExpiredRecordCleanupInterval,
	}

	labels := []string{`name`, `type`}
	m.metrics.readLatency = config.MetricsReporter.Observer(metrics.MetricConf{Path: `backend_read_latency_microseconds`, Labels: labels})
	m.metrics.updateLatency = config.MetricsReporter.Observer(metrics.MetricConf{Path: `backend_update_latency_microseconds`, Labels: labels})
	m.metrics.storageSize = config.MetricsReporter.Gauge(metrics.MetricConf{Path: `backend_storage_size`, Labels: labels})
	m.metrics.deleteLatency = config.MetricsReporter.Observer(metrics.MetricConf{Path: `backend_delete_latency_microseconds`, Labels: labels})

	go m.runCleaner()

	return m
}

func (m *badger) runCleaner() {
	ticker := time.NewTicker(m.expiredRecordCleanupInterval)
	for range ticker.C {
	again:
		err := m.db.RunValueLogGC(1)
		if err == nil {
			goto again
		}
	}
}

func (m *badger) Name() string {
	return m.name
}

func (m *badger) String() string {
	return `memory`
}

func (m *badger) Persistent() bool {
	return true
}

func (m *badger) Set(key []byte, value []byte, _ time.Duration) error {
	defer func(begin time.Time) {
		m.metrics.updateLatency.Observe(float64(time.Since(begin).Nanoseconds()/1e3), map[string]string{`name`: m.Name(), `type`: `memory`})
	}(time.Now())

	return m.db.Update(func(txn *badgerDB.Txn) error {
		return txn.Set(key, value)
	})
}

func (m *badger) Get(key []byte) ([]byte, error) {
	defer func(begin time.Time) {
		m.metrics.readLatency.Observe(float64(time.Since(begin).Nanoseconds()/1e3), map[string]string{`name`: m.Name(), `type`: `memory`})
	}(time.Now())

	var v []byte

	if err := m.db.View(func(txn *badgerDB.Txn) error {
		itm, err := txn.Get(key)
		if err != nil {
			if errors.Is(err, badgerDB.ErrKeyNotFound) {
				v = nil
				return nil
			}
			return err
		}

		val, err := itm.ValueCopy(nil)
		if err != nil {
			return err
		}

		v = val

		return nil
	}); err != nil {
		return nil, err
	}

	return v, nil
}

func (m *badger) PrefixedIterator(keyPrefix []byte) backend.Iterator {
	return m.iterator(keyPrefix)
}

func (m *badger) Iterator() backend.Iterator {
	return m.iterator(nil)
}

func (m *badger) iterator(prefix []byte) backend.Iterator {
	closed := make(chan struct{})
	i := &Iterator{
		itr:    nil,
		closed: closed,
	}

	assigned := make(chan struct{})
	go func() {
		if err := m.db.View(func(txn *badgerDB.Txn) error {
			opts := badgerDB.DefaultIteratorOptions
			if prefix != nil {
				opts.Prefix = prefix
			}
			i.itr = txn.NewIterator(opts)
			close(assigned)

			<-closed
			return nil
		}); err != nil {
			panic(err)
		}
	}()

	<-assigned

	return i
}

func (m *badger) Delete(key []byte) error {
	defer func(begin time.Time) {
		m.metrics.deleteLatency.Observe(float64(time.Since(begin).Nanoseconds()/1e3), map[string]string{`name`: m.Name(), `type`: `memory`})
	}(time.Now())

	return m.db.Update(func(txn *badgerDB.Txn) error {
		err := txn.Delete(key)
		if err != nil && !errors.Is(err, badgerDB.ErrKeyNotFound) {
			return err
		}

		return nil
	})
}

func (m *badger) Destroy() error { panic(`badger does not support Destroy`) }

func (m *badger) SetExpiry(time time.Duration) { panic(`badger does not support SetExpiry`) }

func (m *badger) Close() error {
	return m.db.Close()
}
