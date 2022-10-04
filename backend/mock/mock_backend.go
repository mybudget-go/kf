package mock

import (
	"github.com/gmbyapa/kstream/backend"
	"github.com/gmbyapa/kstream/backend/badger"
	"time"
)

func NewMockBackend(name string, _ time.Duration) backend.Backend {
	conf := badger.NewConfig()
	conf.InMemory = true
	b := badger.NewBadgerBackend(name, conf)
	return b
}
