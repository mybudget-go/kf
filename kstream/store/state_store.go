package store

import (
	"github.com/tryfix/errors"
	"github.com/tryfix/kstream/backend"
	"github.com/tryfix/kstream/data"
	"github.com/tryfix/kstream/kstream/serdes"
)

type stateStore struct {
	name       string
	options    *storeOptions
	backend    backend.Backend
	keyEncoder serdes.SerDes
	valEncoder serdes.SerDes
}

func NewStateStore(name string, keyEncoder serdes.SerDes, valEncoder serdes.SerDes, options ...Options) StateStore {

	configs := storeOptions{}
	configs.apply(options...)

	return &stateStore{
		name:       name,
		keyEncoder: keyEncoder,
		valEncoder: valEncoder,
	}
}

func (s *stateStore) Name() string {
	return s.name
}

func (s *stateStore) Set(key interface{}, value interface{}) error {
	k, err := s.keyEncoder.Serialize(key)
	if err != nil {
		return errors.WithPrevious(err, `key encode err `)
	}

	v, err := s.valEncoder.Serialize(value)
	if err != nil {
		return errors.WithPrevious(err, `key encode err `)
	}

	return s.backend.Set(k, v, 0)
}

func (s *stateStore) Get(key interface{}) (value interface{}, err error) {
	k, err := s.keyEncoder.Serialize(key)
	if err != nil {
		return nil, errors.WithPrevious(err, `key encode err `)
	}

	byts, err := s.options.backend.Get(k)
	if err != nil {
		return nil, errors.WithPrevious(err, `key encode err `)
	}

	v, err := s.valEncoder.Deserialize(byts)
	if err != nil {
		return nil, errors.WithPrevious(err, `value decode err `)
	}

	return v, nil
}

func (s *stateStore) GetAll() ([]*data.Record, error) {
	panic("implement me")
}
