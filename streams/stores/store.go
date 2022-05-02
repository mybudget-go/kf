package stores

import (
	"context"
	"fmt"
	"github.com/gmbyapa/kstream/backend"
	"github.com/gmbyapa/kstream/pkg/errors"
	"github.com/gmbyapa/kstream/streams/encoding"
	"time"
)

type RecordVersionExtractor func(ctx context.Context, key, value interface{}) (version int64, err error)

type RecordVersionWriter func(ctx context.Context, version int64, vIn interface{}) (vOut interface{}, err error)

type Builder func(name string, keyEncoder, valEncoder encoding.Encoder, options ...Option) (Store, error)

type IndexedStoreBuilder func(name string, keyEncoder, valEncoder encoding.Encoder, indexes []Index, options ...Option) (IndexedStore, error)

type Store interface {
	Backend() backend.Backend
	Set(ctx context.Context, key, value interface{}, expiry time.Duration) error
	Delete(ctx context.Context, key interface{}) error
	ReadOnlyStore
}

type ReadOnlyStore interface {
	Name() string
	KeyEncoder() encoding.Encoder
	ValEncoder() encoding.Encoder
	Get(ctx context.Context, key interface{}) (value interface{}, err error)
	Iterator(ctx context.Context) (Iterator, error)
	String() string
	Close() error
}

type LoggableStore interface {
	Store
	EnableLogging() error
	DisableLogging() error
}

type Closable interface {
	Close() error
}

type store struct {
	opts       *StoreOptions
	name       string
	keyEncoder encoding.Encoder
	valEncoder encoding.Encoder
}

func NewStore(name string, keyEncoder encoding.Encoder, valEncoder encoding.Encoder, options ...Option) (Store, error) {
	opts := new(StoreOptions)
	opts.applyDefault()
	opts.apply(options...)

	if opts.backend == nil {
		bk, err := opts.backendBuilder(name)
		if err != nil {
			return nil, errors.Wrap(err, `backend builder error`)
		}
		opts.backend = bk
	}

	store := &store{
		name:       name,
		keyEncoder: keyEncoder,
		valEncoder: valEncoder,
		opts:       opts,
	}

	return store, nil
}

func (s *store) Name() string {
	return s.name
}

func (s *store) String() string {
	return fmt.Sprintf(`Backend: %s`, s.Backend().Name())
}

func (s *store) KeyEncoder() encoding.Encoder {
	return s.keyEncoder
}

func (s *store) ValEncoder() encoding.Encoder {
	return s.valEncoder
}

func (s *store) Backend() backend.Backend {
	return s.opts.backend
}

func (s *store) Set(ctx context.Context, key interface{}, value interface{}, expiry time.Duration) error {

	k, err := s.keyEncoder.Encode(key)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf(`store [%s] key encode error`, s.name))
	}

	// if value is null remove from store (tombstone)
	if value == nil {
		return s.opts.backend.Delete(k)
	}

	v, err := s.valEncoder.Encode(value)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf(`store [%s] value encode err `, s.name))
	}

	return s.opts.backend.Set(k, v, expiry)
}

func (s *store) Get(ctx context.Context, key interface{}) (value interface{}, err error) {

	k, err := s.keyEncoder.Encode(key)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf(`store [%s] key encode err `, s.name))
	}

	byt, err := s.opts.backend.Get(k)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf(`store [%s] value dose not exist `, s.name))
	}

	if len(byt) < 1 {
		return nil, nil
	}

	v, err := s.valEncoder.Decode(byt)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf(`store [%s] value decode err `, s.name))
	}

	return v, nil
}

func (s *store) Iterator(ctx context.Context) (Iterator, error) {

	i := s.opts.backend.Iterator()
	i.SeekToFirst()

	return &iterator{
		i:          i,
		keyEncoder: s.keyEncoder,
		valEncoder: s.valEncoder,
	}, nil
}

func (s *store) Delete(ctx context.Context, key interface{}) (err error) {
	k, err := s.keyEncoder.Encode(key)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf(`store [%s] key encode err `, s.name))
	}

	return s.opts.backend.Delete(k)
}

func (s *store) Close() error {
	return s.opts.backend.Close()
}
