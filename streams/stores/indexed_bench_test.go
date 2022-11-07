package stores

import (
	"math/rand"
	"strconv"
	"testing"
)

func BenchmarkHashIndex_Write(b *testing.B) {
	idx := buildIndexB(b)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := idx.Write(strconv.Itoa(rand.Intn(100000)+1), `111,222`); err != nil {
				b.Error(err)
			}
		}

	})
}

func BenchmarkHashIndex_Read(b *testing.B) {
	idx := buildIndexB(b)

	for i := 1; i < 1000; i++ {
		if err := idx.Write(strconv.Itoa(i), `111,222`); err != nil {
			b.Error(err)
		}
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := idx.Read(`111`); err != nil {
				b.Error(err)
			}
		}

	})
}
