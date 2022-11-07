package stores

import (
	"context"
	"github.com/gmbyapa/kstream/backend"
	"github.com/gmbyapa/kstream/backend/memory"
	"github.com/gmbyapa/kstream/backend/pebble"
	"github.com/gmbyapa/kstream/streams/encoding"
	"github.com/tryfix/metrics"
	"os"
	"strconv"
	"strings"
	"testing"
)

func buildIndexB(b *testing.B) Index {
	var bkBuilder backend.Builder = func(name string) (backend.Backend, error) {
		conf := pebble.NewConfig()
		dir, err := os.MkdirTemp(os.TempDir(), `*`)
		if err != nil {
			return nil, err
		}
		conf.Dir = dir

		return pebble.NewPebbleBackend(name, conf)

	}

	idx, err := NewIndex(`foo`, func(key string, val interface{}) (idx string) {
		return strings.Split(val.(string), `,`)[0]
	}, IndexWithBackend(bkBuilder))
	if err != nil {
		b.Error(err)
	}

	return idx
}

func buildIndexT(t *testing.T) Index {
	var bkBuilder backend.Builder = func(name string) (backend.Backend, error) {
		conf := pebble.NewConfig()
		dir, err := os.MkdirTemp(os.TempDir(), `*`)
		if err != nil {
			return nil, err
		}
		conf.Dir = dir

		return pebble.NewPebbleBackend(name, conf)

	}

	idx, err := NewIndex(`foo`, func(key string, val interface{}) (idx string) {
		return strings.Split(val.(string), `,`)[0]
	}, IndexWithBackend(bkBuilder))
	if err != nil {
		t.Error(err)
	}

	return idx
}

func BenchmarkUpdateIndexes(b *testing.B) {
	idx := buildIndexB(b)

	conf := memory.NewConfig()
	conf.MetricsReporter = metrics.NoopReporter()
	st, err := NewIndexedStore(
		`foo`,
		encoding.StringEncoder{},
		encoding.StringEncoder{},
		[]Index{idx},
		WithBackend(memory.NewMemoryBackend(`foo`, conf)))
	if err != nil {
		b.Error(err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := UpdateIndexes(context.Background(), st, strconv.Itoa(1), `111,222`); err != nil {
				b.Error(err)
			}

			if err := st.Set(context.Background(), strconv.Itoa(1), `111,222`, 0); err != nil {
				b.Error(err)
			}
		}
	})
}
