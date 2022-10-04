package state_stores

import (
	"bytes"
	"context"
	"fmt"
	"github.com/gmbyapa/kstream/backend/memory"
	"github.com/gmbyapa/kstream/streams/encoding"
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

	// tombstone record
	if value == nil {
		str.cache.Delete(keyByt)
		return nil
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

	if !str.cache.Has(keyByt) {
		return str.Store.Get(ctx, key)
	}

	if valByt := str.cache.Read(keyByt); valByt != nil {
		val, err := str.ValEncoder().Decode(valByt)
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf(`store [%s] value encode error`, str))
		}

		return val, nil
	}

	return nil, nil
}

func (str *StateStore) Iterator(ctx context.Context) (stores.Iterator, error) {
	return str.iterator(ctx, nil, nil)
}

func (str *StateStore) PrefixedIterator(ctx context.Context, keyPrefix interface{}, prefixEncoder encoding.Encoder) (stores.Iterator, error) {
	return str.iterator(ctx, keyPrefix, prefixEncoder)
}

func (str *StateStore) iterator(ctx context.Context, prefix interface{}, prefixEncoder encoding.Encoder) (stores.Iterator, error) {
	var itr stores.Iterator
	var prefixByt []byte
	if prefix != nil {
		byt, err := prefixEncoder.Encode(prefix)
		if err != nil {
			return nil, err
		}
		prefixByt = byt

		i, err := str.Store.PrefixedIterator(ctx, prefix, prefixEncoder)
		// TODO handle error
		if err != nil {
			return nil, err
		}

		itr = i
	} else {
		i, err := str.Store.Iterator(ctx)
		// TODO handle error
		if err != nil {
			return nil, err
		}
		itr = i
	}

	records := make(map[string][]byte)
	for itr.SeekToFirst(); itr.Valid(); itr.Next() {
		k, err := itr.Key()
		if err != nil {
			return nil, err
		}

		keyByt, err := str.KeyEncoder().Encode(k)
		if err != nil {
			return nil, err
		}

		v, err := itr.Value()
		if err != nil {
			return nil, err
		}

		valByt, err := str.ValEncoder().Encode(v)
		if err != nil {
			return nil, err
		}

		records[string(keyByt)] = valByt
	}

	for cachedK, cachedV := range str.cache.records {
		if str.cache.Deleted(cachedK) {
			delete(records, cachedK)
			continue
		}

		if prefix != nil && !bytes.HasPrefix([]byte(cachedK), prefixByt) {
			continue
		}
		records[cachedK] = cachedV
	}

	recsArr := make([]memory.ByteRecord, len(records))
	var i int
	for key, val := range records {
		recsArr[i] = memory.ByteRecord{
			Key:   []byte(key),
			Value: val,
		}
		i++
	}

	return stores.NewIterator(memory.NewMemoryIterator(recsArr), str.KeyEncoder(), str.ValEncoder()), nil
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
