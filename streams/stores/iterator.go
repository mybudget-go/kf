package stores

import (
	"github.com/gmbyapa/kstream/backend"
	"github.com/gmbyapa/kstream/streams/encoding"
)

type Iterator interface {
	SeekToFirst()
	SeekToLast()
	//Seek(key interface{}) error
	Next()
	Prev()
	Close()
	Key() (interface{}, error)
	Value() (interface{}, error)
	Valid() bool
	Error() error
	BackendIterator() backend.Iterator
}

type iterator struct {
	i          backend.Iterator
	keyEncoder encoding.Encoder
	valEncoder encoding.Encoder
}

func (i *iterator) SeekToFirst() {
	i.i.SeekToFirst()
}

func (i *iterator) SeekToLast() {
	i.i.SeekToLast()
}

func (i *iterator) Seek(key interface{}) error {
	k, err := i.keyEncoder.Encode(key)
	if err != nil {
		return err
	}

	i.i.Seek(k)
	return nil
}

func (i *iterator) Next() {
	i.i.Next()
}

func (i *iterator) Prev() {
	i.i.Prev()
}

func (i *iterator) Close() {
	i.i.Close()
}

func (i *iterator) Key() (interface{}, error) {
	k := i.i.Key()
	if len(k) < 1 {
		return nil, nil
	}

	return i.keyEncoder.Decode(k)
}

func (i *iterator) Value() (interface{}, error) {
	v := i.i.Value()
	if len(v) < 1 {
		return nil, nil
	}

	return i.valEncoder.Decode(v)
}

func (i *iterator) Valid() bool {
	return i.i.Valid()
}

func (i *iterator) Error() error {
	return i.i.Error()
}

func (i *iterator) BackendIterator() backend.Iterator {
	return i.i
}
