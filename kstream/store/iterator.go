package store

import (
	"github.com/tryfix/kstream/backend"
	"github.com/tryfix/kstream/kstream/serdes"
)

type Iterator interface {
	//SeekToFirst()
	//SeekToLast()
	//Seek(key interface{}) error
	Next() bool
	//Prev()
	Close()
	Key() (interface{}, error)
	Value() (interface{}, error)
	Valid() bool
	Error() error
}

type iterator struct {
	i          backend.Iterator
	keyEncoder serdes.SerDes
	valEncoder serdes.SerDes
}

func (i *iterator) SeekToFirst() {
	i.i.SeekToFirst()
}

func (i *iterator) SeekToLast() {
	i.i.SeekToLast()
}

func (i *iterator) Seek(key interface{}) error {
	k, err := i.keyEncoder.Serialize(key)
	if err != nil {
		return err
	}

	i.i.Seek(k)
	return nil
}

func (i *iterator) Next() bool {
	i.i.Next()
	return i.i.Valid()
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

	return i.keyEncoder.Deserialize(k)
}

func (i *iterator) Value() (interface{}, error) {
	v := i.i.Value()
	if len(v) < 1 {
		return nil, nil
	}

	return i.valEncoder.Serialize(v)
}

func (i *iterator) Valid() bool {
	return i.i.Valid()
}

func (i *iterator) Error() error {
	return i.i.Error()
}
