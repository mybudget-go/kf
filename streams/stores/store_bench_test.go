package stores

import (
	"context"
	"github.com/gmbyapa/kstream/backend"
	"github.com/gmbyapa/kstream/backend/badger"
	"github.com/gmbyapa/kstream/backend/memory"
	"github.com/gmbyapa/kstream/streams/encoding"
	"math/rand"
	"testing"
)

func makeTestBackend() backend.Backend {
	conf := badger.NewConfig()
	conf.InMemory = true
	backend := badger.NewBadgerBackend(`mock`, conf)
	backend = memory.NewMemoryBackend(`mock`, memory.NewConfig())

	return backend
}

func makeTestBenchStore(b *testing.B) Store {
	stor, err := NewStore(
		`test_store`,
		encoding.IntEncoder{},
		encoding.StringEncoder{},
		WithBackend(makeTestBackend()))
	if err != nil {
		b.Error(err)
	}

	return stor
}

func BenchmarkDefaultStore_Set(b *testing.B) {

	store := makeTestBenchStore(b)
	ctx := context.Background()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := store.Set(ctx, rand.Intn(10000000), `100`, 0); err != nil {
				b.Error(err)
			}
		}
	})
}

func BenchmarkDefaultStore_Get(b *testing.B) {
	store := makeTestBenchStore(b)
	ctx := context.Background()

	for i := 1; i < 999999; i++ {
		if err := store.Set(ctx, rand.Intn(i), `100`, 0); err != nil {
			b.Error(err)
		}
	}

	b.ResetTimer()
	b.StartTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := store.Get(ctx, rand.Intn(999998)+1); err != nil {
				b.Error(err)
			}
		}
	})
}

func BenchmarkDefaultStore_Delete(b *testing.B) {
	store := makeTestBenchStore(b)
	ctx := context.Background()

	for i := 1; i <= 999999; i++ {
		if err := store.Set(ctx, rand.Intn(i), `100`, 0); err != nil {
			b.Error(err)
		}
	}

	b.ResetTimer()
	b.StartTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := store.Delete(ctx, rand.Intn(999998)+1); err != nil {
				b.Error(err)
			}
		}
	})
}
