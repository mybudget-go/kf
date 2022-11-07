package stores

import (
	"context"
	"github.com/gmbyapa/kstream/pkg/errors"
)

func UpdateIndexes(ctx context.Context, store IndexedStore, key, val interface{}) error {
	for _, idx := range store.Indexes() {
		// get the previous value for the indexed key
		valPrv, err := store.Get(ctx, key)
		if err != nil {
			return errors.Wrapf(err, `cannot fetch value, key:%v, store:%s`, key, store.Name())
		}

		kByt, err := store.KeyEncoder().Encode(key)
		if err != nil {
			return err
		}

		// if previous exists and different from current value
		// eg: val.name=foo -> val.name=bar then find index for foo and delete
		if valPrv != nil {
			hash := idx.Hash(string(kByt), valPrv)
			// check if value already indexed
			indexed, err := idx.KeyIndexed(hash, string(kByt))
			if err != nil {
				return errors.Wrapf(err, `value index check failed, key:%v, store:%s`, key, store.Name())
			}

			// if already indexed remove from previous index
			if indexed {
				if err := idx.Delete(string(kByt), valPrv); err != nil {
					return errors.Wrapf(err, `index delete failed, key:%v, store:%s`, key, store.Name())
				}
			}
		}

		if err := idx.Write(string(kByt), val); err != nil {
			return errors.Wrapf(err, `index write failed, key:%v, store:%s`, key, store.Name())
		}
	}

	return nil
}

func DeleteIndexes(ctx context.Context, store IndexedStore, key interface{}) error {
	val, err := store.Get(ctx, key)
	if err != nil {
		return errors.Wrapf(err, `indexed value delete failed due to record fetch error`)
	}

	kByt, err := store.KeyEncoder().Encode(key)
	if err != nil {
		return err
	}

	if val != nil {
		for _, idx := range store.Indexes() {
			if err := idx.Delete(string(kByt), val); err != nil {
				return errors.Wrapf(err, `indexed value delete error`)
			}
		}
	}

	return nil
}
