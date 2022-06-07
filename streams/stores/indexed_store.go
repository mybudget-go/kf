package stores

import (
	"context"
	nativeErrors "errors"
	"fmt"
	"github.com/gmbyapa/kstream/streams/encoding"

	"sync"
	"time"
)

type Index interface {
	String() string
	Write(key, value interface{}) error
	Hash(key, val interface{}) (hash interface{})
	Delete(key, value interface{}) error
	Read(index interface{}) ([]interface{}, error)
	Keys() []interface{}
	Values() map[interface{}][]interface{}
	ValueIndexed(index, value interface{}) (bool, error)
}

type IndexedStore interface {
	Store
	GetIndex(ctx context.Context, name string) (Index, error)
	Indexes() []Index
	GetIndexedRecords(ctx context.Context, indexName string, key interface{}) (Iterator, error)
}

type indexedStore struct {
	Store
	indexes map[string]Index

	mu *sync.Mutex
}

func NewIndexedStore(name string, keyEncoder, valEncoder encoding.Encoder, indexes []Index, options ...Option) (IndexedStore, error) {
	store, err := NewStore(name, keyEncoder, valEncoder, options...)
	if err != nil {
		return nil, err
	}

	idxs := make(map[string]Index)
	for _, idx := range indexes {
		idxs[idx.String()] = idx
	}

	return &indexedStore{
		Store:   store,
		indexes: idxs,
		mu:      new(sync.Mutex),
	}, nil
}

func (i *indexedStore) Set(ctx context.Context, key, val interface{}, expiry time.Duration) error {
	// set indexes
	i.mu.Lock()
	defer i.mu.Unlock()

	if err := UpdateIndexes(ctx, i, key, val); err != nil {
		return err // TODO wrap error
	}

	return i.Store.Set(ctx, key, val, expiry)
}

func (i *indexedStore) Delete(ctx context.Context, key interface{}) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if err := DeleteIndexes(ctx, i, key); err != nil {
		return err // TODO wrap error
	}

	return i.Store.Delete(ctx, key)
}

func (i *indexedStore) GetIndex(_ context.Context, name string) (Index, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	idx, ok := i.indexes[name]
	if !ok {
		return nil, fmt.Errorf(`associate [%s] does not exist`, name)
	}

	return idx, nil
}

func (i *indexedStore) Indexes() []Index {
	var idxs []Index
	for _, idx := range i.indexes {
		idxs = append(idxs, idx)
	}

	return idxs
}

func (i *indexedStore) GetIndexedRecords(ctx context.Context, indexName string, key interface{}) (Iterator, error) {
	i.mu.Lock()
	idx, ok := i.indexes[indexName]
	i.mu.Unlock()

	if !ok {
		return nil, fmt.Errorf(`associate [%s] does not exist`, indexName)
	}

	itr := &indexIterator{
		currentKey: 0,
		valid:      false,
	}

	indexes, err := idx.Read(key)
	if err != nil {
		if nativeErrors.Is(err, UnknownIndex) {
			return itr, nil
		}
		return nil, err
	}

	for _, index := range indexes {
		record, err := i.Get(ctx, index)
		if err != nil {
			return nil, err
		}
		itr.records = append(itr.records, &keyVal{
			key: index,
			val: record,
		})
	}

	return itr, nil
}
