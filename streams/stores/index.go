package stores

import (
	"fmt"
	"github.com/gmbyapa/kstream/backend"
	"github.com/gmbyapa/kstream/backend/pebble"
)

type KeyMapper func(key string, val interface{}) (idx string)

type IndexOption func(config *index)

func (i *index) applyDefault() {
	conf := pebble.NewConfig()
	i.backendBuilder = func(name string) (backend.Backend, error) {
		return pebble.NewPebbleBackend(name, conf)
	}
}

func IndexWithBackend(builder backend.Builder) IndexOption {
	return func(config *index) {
		config.backendBuilder = builder
	}
}

type index struct {
	mapper         KeyMapper
	name           string
	backendBuilder backend.Builder
	backend        backend.Backend
}

func NewIndex(name string, mapper KeyMapper, opts ...IndexOption) (Index, error) {
	idx := &index{
		mapper: mapper,
		name:   name,
	}

	idx.applyDefault()
	for _, opt := range opts {
		opt(idx)
	}

	bk, err := idx.backendBuilder(fmt.Sprintf(`idx_%s`, name))
	if err != nil {
		return nil, err
	}

	idx.backend = bk

	return idx, nil
}

func (i *index) String() string {
	return i.name
}

func (i *index) Write(key string, value interface{}) error {
	hashKey := i.mapper(key, value)
	return i.backend.Set([]byte(hashKey+key), []byte(key), 0)
}

func (i *index) KeyIndexed(index, key string) (bool, error) {
	kIdxed, err := i.backend.Get([]byte(index + key))
	if err != nil {
		return false, err
	}

	return kIdxed != nil, nil
}

func (i *index) Hash(key string, val interface{}) (hash string) {
	return i.mapper(key, val)
}

func (i *index) Delete(key string, value interface{}) error {
	hashKey := i.mapper(key, value)
	return i.backend.Delete([]byte(hashKey + key))
}

func (i *index) Values(key string) ([]string, error) {
	itr := i.backend.PrefixedIterator([]byte(key))
	var vals []string

	for itr.SeekToFirst(); itr.Valid(); itr.Next() {
		vals = append(vals, string(itr.Value()))
	}

	return vals, nil
}

func (i *index) Keys() ([]string, error) {
	itr := i.backend.Iterator()
	var keys []string

	for itr.SeekToFirst(); itr.Valid(); itr.Next() {
		keys = append(keys, string(itr.Key()))
	}

	return keys, nil
}

func (i *index) Read(key string) (backend.Iterator, error) {
	return i.backend.PrefixedIterator([]byte(key)), nil
}

func (i *index) Close() error {
	return i.backend.Close()
}
