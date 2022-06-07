package stores

import (
	"context"
	"github.com/gmbyapa/kstream/backend"
	"github.com/gmbyapa/kstream/backend/memory"
	"github.com/gmbyapa/kstream/streams/encoding"
	"github.com/tryfix/metrics"
	"math/rand"
	"strconv"
	"strings"
	"testing"
)

func BenchmarkIndexedStore_Set(b *testing.B) {
	idx := NewIndex(`foo`, func(key, val interface{}) (idx interface{}) {
		return strings.Split(val.(string), `,`)[0]
	})

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
			if err := st.Set(context.Background(), strconv.Itoa(rand.Intn(99999)+1), `111,222`, 0); err != nil {
				b.Error(err)
			}
		}
	})
}

func BenchmarkIndexedStore_GetIndexedRecords(b *testing.B) {
	idxStore := NewMockStore(`foo`, encoding.StringEncoder{}, encoding.StringEncoder{}, backend.NewMockBackend(`foo`, 0))
	for i := 1; i < 99909; i++ {
		compKey := strconv.Itoa(rand.Intn(4)+1) + `:` + strconv.Itoa(i)
		if err := idxStore.Set(context.Background(), strconv.Itoa(i), compKey, 0); err != nil {
			b.Error(err)
		}
	}

	idx := NewIndex(`foo`, func(key, val interface{}) (idx interface{}) {
		return strings.Split(val.(string), `:`)[0]
	})

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

	for i := 1; i < 99909; i++ {
		compKey := strconv.Itoa(rand.Intn(4)+1) + `:` + strconv.Itoa(i)
		if err := st.Set(context.Background(), strconv.Itoa(i), compKey, 0); err != nil {
			b.Error(err)
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := st.GetIndexedRecords(context.Background(), `foo`, strconv.Itoa(rand.Intn(4)+1)); err != nil {
				b.Error(err)
			}
		}
	})
}
