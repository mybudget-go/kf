package store

import (
	"context"
	"fmt"
	"github.com/tryfix/errors"
	"github.com/tryfix/kstream/backend"
	"github.com/tryfix/kstream/data"
	"github.com/tryfix/kstream/kstream/serdes"
	"github.com/tryfix/log"
	"time"
)

type RecordVersionExtractor func(ctx context.Context, key, value interface{}) (version int64, err error)
type RecordVersionWriter func(ctx context.Context, version int64, vIn interface{}) (vOut interface{}, err error)

type Builder func(name string, keyEncoder, valEncoder serdes.SerDes, options ...Options) (Store, error)
type IndexedStoreBuilder func(name string, keyEncoder, valEncoder serdes.SerDes, indexes []Index, options ...Options) (IndexedStore, error)
type StateStoreBuilder func(name string, keyEncoder, valEncoder serdes.SerDes, options ...Options) StateStore

type Store interface {
	Name() string
	Backend() backend.Backend
	KeyEncoder() serdes.SerDes
	ValEncoder() serdes.SerDes
	Set(ctx context.Context, key, value interface{}, expiry time.Duration) error
	Get(ctx context.Context, key interface{}) (value interface{}, err error)
	GetRange(ctx context.Context, fromKey, toKey interface{}) (map[interface{}]interface{}, error)
	GetAll(ctx context.Context) (Iterator, error)
	Delete(ctx context.Context, key interface{}) error
	String() string
}

type StateStore interface {
	Name() string
	Set(key, value interface{}) error
	Get(key interface{}) (value interface{}, err error)
	GetAll() ([]*data.Record, error)
}

type store struct {
	backend    backend.Backend
	name       string
	logger     log.Logger
	keyEncoder serdes.SerDes
	valEncoder serdes.SerDes
}

func NewStore(name string, keyEncoder serdes.SerDes, valEncoder serdes.SerDes, options ...Options) (Store, error) {

	opts := new(storeOptions)
	opts.apply(options...)

	if opts.backend == nil {
		bk, err := opts.backendBuilder(name)
		if err != nil {
			opts.logger.Fatal(`k-stream.Store.Registry`, fmt.Sprintf(`backend builder error - %+v`, err))
		}
		opts.backend = bk
	}

	store := &store{
		name:       name,
		keyEncoder: keyEncoder,
		logger:     opts.logger,
		valEncoder: valEncoder,
		backend:    opts.backend,
	}

	store.backend.SetExpiry(opts.expiry)

	if opts.changelogEnable {
		panic(`not yet implemented`)
		/*p, err := producer.DefaultBuilder(&producer.Option{
			Partitioner: producer.Random,
		})
		if err != nil {
			return nil, err
		}

		topic := name + `_store`
		cLog, err := changelog.DefaultBuilder(name, topic, -1, changelog.Producer(p))
		opts.changelog = cLog

		return &recoverableStore{
			Store:     store,
			changelog: cLog,
		}, nil

		store.backend.SetExpiry(opts.expiry)*/
	}

	opts.logger.Info(
		fmt.Sprintf(`default store [%s] inited`, name))

	return store, nil
}

func (s *store) Name() string {
	return s.name
}

func (s *store) String() string {
	return fmt.Sprintf(`Backend: %s`, s.Backend().Name())
}

func (s *store) KeyEncoder() serdes.SerDes {
	return s.keyEncoder
}

func (s *store) ValEncoder() serdes.SerDes {
	return s.valEncoder
}

func (s *store) Backend() backend.Backend {
	return s.backend
}

func (s *store) Set(ctx context.Context, key interface{}, value interface{}, expiry time.Duration) error {

	k, err := s.keyEncoder.Serialize(key)
	if err != nil {
		return errors.WithPrevious(err, fmt.Sprintf(`store [%s] key encode error`, s.name))
	}

	// if value is null remove from store (tombstone)
	if value == nil {
		return s.backend.Delete(k)
	}

	v, err := s.valEncoder.Serialize(value)
	if err != nil {
		return errors.WithPrevious(err, fmt.Sprintf(`store [%s] key encode err `, s.name))
	}

	// if changelog enable write record to the changelog
	//if s.changelog != nil {
	//	record, err := kContext.RecordFromContext(ctx, k, v)
	//	if err != nil {
	//		return err
	//	}
	//
	//	if err := s.changelog.Put(ctx, record); err != nil {
	//		return err
	//	}
	//}

	return s.backend.Set(k, v, expiry)
}

func (s *store) Get(ctx context.Context, key interface{}) (value interface{}, err error) {

	k, err := s.keyEncoder.Serialize(key)
	if err != nil {
		return nil, errors.WithPrevious(err, fmt.Sprintf(`store [%s] key encode err `, s.name))
	}

	byt, err := s.backend.Get(k)
	if err != nil {
		return nil, errors.WithPrevious(err, fmt.Sprintf(`store [%s] value dose not exist `, s.name))
	}

	if len(byt) < 1 {
		return nil, nil
	}

	v, err := s.valEncoder.Deserialize(byt)
	if err != nil {
		return nil, errors.WithPrevious(err, fmt.Sprintf(`store [%s] value decode err `, s.name))
	}

	return v, nil
}

func (s *store) GetRange(ctx context.Context, fromKey interface{}, toKey interface{}) (map[interface{}]interface{}, error) {
	i := s.backend.Iterator()
	i.SeekToFirst()

	vals := make(map[interface{}]interface{})

	for i.Valid() {
		if i.Error() != nil {
			return nil, errors.WithPrevious(i.Error(), fmt.Sprintf(`store [%s] backend key i error `, s.name))
		}

		k, err := s.keyEncoder.Deserialize(i.Key())
		if err != nil {
			return nil, errors.WithPrevious(err, fmt.Sprintf(`store [%s] value decode err `, s.name))
		}

		if len(i.Value()) < 1 {
			vals[k] = nil
			i.Next()
		}

		v, err := s.valEncoder.Deserialize(i.Value())
		if err != nil {
			return nil, errors.WithPrevious(err, fmt.Sprintf(`store [%s] value decode err `, s.name))
		}

		vals[k] = v
		i.Next()
	}

	return vals, nil
}

func (s *store) GetAll(ctx context.Context) (Iterator, error) {

	i := s.backend.Iterator()
	i.SeekToFirst()

	return &iterator{
		i:          i,
		keyEncoder: s.keyEncoder,
		valEncoder: s.valEncoder,
	}, nil
}

func (s *store) Delete(ctx context.Context, key interface{}) (err error) {
	k, err := s.keyEncoder.Serialize(key)
	if err != nil {
		return errors.WithPrevious(err, fmt.Sprintf(`store [%s] key encode err `, s.name))
	}

	// if changelog enable delete record from changelog
	//if s.changelog != nil {
	//	record, err := kContext.RecordFromContext(ctx, k, nil)
	//	if err != nil {
	//		return err
	//	}
	//
	//	if err := s.changelog.Delete(ctx, record); err != nil {
	//		return err
	//	}
	//}

	return s.backend.Delete(k)
}
