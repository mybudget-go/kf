/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package pebble

import (
	"fmt"
	pebbleDB "github.com/cockroachdb/pebble"
	"github.com/gmbyapa/kstream/backend"
	"github.com/gmbyapa/kstream/pkg/errors"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
	"time"
)

type Config struct {
	MetricsReporter metrics.Reporter
	Dir             string
	Options         *pebbleDB.Options
}

func NewConfig() *Config {
	conf := new(Config)
	conf.Dir = `storage/pebble`
	conf.Options = &pebbleDB.Options{}
	conf.parse()

	return conf
}

func (c *Config) parse() {
	if c.MetricsReporter == nil {
		c.MetricsReporter = metrics.NoopReporter()
	}
}

type Pebble struct {
	logger  log.Logger
	pebble  *pebbleDB.DB
	metrics struct {
		readLatency             metrics.Observer
		updateLatency           metrics.Observer
		iteratorLatency         metrics.Observer
		prefixedIteratorLatency metrics.Observer
		deleteLatency           metrics.Observer
		storageSize             metrics.Gauge
	}
}

func Builder(config *Config) backend.Builder {
	return func(name string) (backend backend.Backend, err error) {
		return NewPebbleBackend(name, config)
	}
}

func NewPebbleBackend(name string, config *Config) (backend.Backend, error) {
	pb, err := pebbleDB.Open(fmt.Sprintf(`%s/%s`, config.Dir, name), config.Options)
	if err != nil {
		return nil, err
	}

	m := &Pebble{}
	m.pebble = pb

	labels := []string{`name`, `type`}
	m.metrics.readLatency = config.MetricsReporter.Observer(metrics.MetricConf{Path: `backend_read_latency_microseconds`, Labels: labels})
	m.metrics.iteratorLatency = config.MetricsReporter.Observer(metrics.MetricConf{Path: `backend_read_iterator_latency_microseconds`, Labels: labels})
	m.metrics.prefixedIteratorLatency = config.MetricsReporter.Observer(metrics.MetricConf{Path: `backend_read_prefix_iterator_latency_microseconds`, Labels: labels})
	m.metrics.updateLatency = config.MetricsReporter.Observer(metrics.MetricConf{Path: `backend_update_latency_microseconds`, Labels: labels})
	m.metrics.storageSize = config.MetricsReporter.Gauge(metrics.MetricConf{Path: `backend_storage_size`, Labels: labels})
	m.metrics.deleteLatency = config.MetricsReporter.Observer(metrics.MetricConf{Path: `backend_delete_latency_microseconds`, Labels: labels})

	return m, nil
}

func (p *Pebble) Name() string {
	return `pebble`
}

func (p *Pebble) String() string {
	return `pebble`
}

func (p *Pebble) Persistent() bool {
	return false
}

func (p *Pebble) Set(key []byte, value []byte, expiry time.Duration) error {
	defer func(begin time.Time) {
		p.metrics.updateLatency.Observe(
			float64(time.Since(begin).Nanoseconds()/1e3), map[string]string{`name`: p.Name(), `type`: `memory`})
	}(time.Now())

	return p.pebble.Set(key, value, pebbleDB.NoSync)
}

func (p *Pebble) Get(key []byte) ([]byte, error) {
	defer func(begin time.Time) {
		p.metrics.readLatency.Observe(
			float64(time.Since(begin).Nanoseconds()/1e3), map[string]string{`name`: p.Name(), `type`: `memory`})
	}(time.Now())

	valP, buf, err := p.pebble.Get(key)
	if err != nil {
		if errors.Is(err, pebbleDB.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}

	val := make([]byte, len(valP))

	copy(val, valP)

	if err := buf.Close(); err != nil {
		return nil, err
	}

	return val, nil
}

func (p *Pebble) PrefixedIterator(keyPrefix []byte) backend.Iterator {
	defer func(begin time.Time) {
		p.metrics.prefixedIteratorLatency.Observe(float64(time.Since(begin).Nanoseconds()/1e3), map[string]string{`name`: p.Name(), `type`: `memory`})
	}(time.Now())

	opts := new(pebbleDB.IterOptions)
	opts.LowerBound = keyPrefix
	opts.UpperBound = keyUpperBound(keyPrefix)
	return &Iterator{itr: p.pebble.NewIter(opts)}
}

func (p *Pebble) Iterator() backend.Iterator {
	defer func(begin time.Time) {
		p.metrics.prefixedIteratorLatency.Observe(float64(time.Since(begin).Nanoseconds()/1e3), map[string]string{`name`: p.Name(), `type`: `memory`})
	}(time.Now())

	return &Iterator{itr: p.pebble.NewIter(new(pebbleDB.IterOptions))}
}

func (p *Pebble) Delete(key []byte) error {
	defer func(begin time.Time) {
		p.metrics.deleteLatency.Observe(float64(time.Since(begin).Nanoseconds()/1e3), map[string]string{`name`: p.Name(), `type`: `memory`})
	}(time.Now())

	return p.pebble.Delete(key, pebbleDB.NoSync)
}

func (p *Pebble) Destroy() error { return nil }

func (p *Pebble) SetExpiry(_ time.Duration) {}

func (p *Pebble) reportMetricsSize() {}

func (p *Pebble) Close() error {
	return p.pebble.Close()
}

func keyUpperBound(b []byte) []byte {
	end := make([]byte, len(b))
	copy(end, b)
	for i := len(end) - 1; i >= 0; i-- {
		end[i] = end[i] + 1
		if end[i] != 0 {
			return end[:i+1]
		}
	}
	return nil // no upper-bound
}
