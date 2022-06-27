package stores

import (
	"context"
	"github.com/gmbyapa/kstream/backend/memory"
	"github.com/gmbyapa/kstream/streams/encoding"
	"github.com/tryfix/metrics"
	"strconv"
	"strings"
	"testing"
)

func BenchmarkUpdateIndexes(b *testing.B) {
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
			if err := UpdateIndexes(context.Background(), st, strconv.Itoa(1), `111,222`); err != nil {
				b.Error(err)
			}

			if err := st.Set(context.Background(), strconv.Itoa(1), `111,222`, 0); err != nil {
				b.Error(err)
			}
		}
	})
}
