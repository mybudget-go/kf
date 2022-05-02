package state_stores

import (
	"context"
	"fmt"
	"time"

	"github.com/gmbyapa/kstream/pkg/errors"
	"github.com/gmbyapa/kstream/streams/stores"
	"github.com/gmbyapa/kstream/streams/topology"
)

type StateStore struct {
	stores.Store
	topology.ChangelogSyncer
	cache *Cache
}

func (str *StateStore) Set(_ context.Context, key, value interface{}, _ time.Duration) error {
	keyByt, err := str.KeyEncoder().Encode(key)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf(`store [%s] key encode error`, str))
	}

	valByt, err := str.ValEncoder().Encode(value)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf(`store [%s] value encode err `, str))
	}

	str.cache.Write(keyByt, valByt)

	return nil
}

func (str *StateStore) Get(ctx context.Context, key interface{}) (interface{}, error) {
	keyByt, err := str.KeyEncoder().Encode(key)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf(`store [%s] key encode error`, str))
	}

	if valByt := str.cache.Read(keyByt); valByt != nil {
		val, err := str.ValEncoder().Decode(valByt)
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf(`store [%s] value encode error`, str))
		}

		return val, nil
	}

	return str.Store.Get(ctx, key)
}

func (str *StateStore) Delete(_ context.Context, key interface{}) error {
	keyByt, err := str.KeyEncoder().Encode(key)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf(`store [%s] key encode error`, str))
	}

	str.cache.Delete(keyByt)

	return nil
}

func (str *StateStore) Flush() error {
	for keyByt, valByt := range str.cache.records {
		if valByt == nil {
			if err := str.Store.Backend().Delete([]byte(keyByt)); err != nil {
				return errors.Wrapf(err, `backend store flush error. store:%s`, str)
			}
			continue
		}

		if err := str.Store.Backend().Set([]byte(keyByt), valByt, 0); err != nil {
			return err // TODO handle error
		}
	}

	// Purge store cache
	str.Purge()

	return nil
}

func (str *StateStore) Purge() {
	str.cache.Purge()
}
