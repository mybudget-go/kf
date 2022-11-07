package stores

import (
	"context"
	"github.com/gmbyapa/kstream/backend"
)

type indexIterator struct {
	store         ReadOnlyStore
	indexIterator backend.Iterator
}

func (i *indexIterator) SeekToFirst() {
	i.indexIterator.SeekToFirst()
}

func (i *indexIterator) Next() {
	i.indexIterator.Next()
}

func (i *indexIterator) Close() {
	i.indexIterator.Close()
}

func (i *indexIterator) Key() (interface{}, error) {
	return string(i.indexIterator.Key()), nil
}

func (i *indexIterator) Value() (interface{}, error) {
	return i.store.Get(context.Background(), string(i.indexIterator.Value()))
}

func (i *indexIterator) Valid() bool {
	return i.indexIterator.Valid()
}

func (i *indexIterator) Error() error {
	return i.indexIterator.Error()
}
