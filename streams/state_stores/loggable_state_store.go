package state_stores

import (
	"context"
	"fmt"
	"time"

	"github.com/gmbyapa/kstream/pkg/errors"
	"github.com/gmbyapa/kstream/streams/topology"
)

type loggableStateStoreInstance struct {
	*stateStore
	topology.ChangeLogger
}

func (str *loggableStateStoreInstance) Set(ctx context.Context, key, value interface{}, expiry time.Duration) error {
	keyByt, err := str.KeyEncoder().Encode(key)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf(`store [%s] key encode error`, str))
	}

	valByt, err := str.ValEncoder().Encode(value)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf(`store [%s] value encode err `, str))
	}

	if err := str.Log(ctx, keyByt, valByt); err != nil {
		return errors.Wrapf(err, `store [%s] changelog write failed`, str)
	}

	str.cache.Write(keyByt, valByt)
	return nil
}

func (str *loggableStateStoreInstance) Delete(ctx context.Context, key interface{}) error {
	keyByt, err := str.KeyEncoder().Encode(key)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf(`store [%s] key encode error`, str))
	}

	if err := str.Log(ctx, keyByt, nil); err != nil {
		return errors.Wrapf(err, `store [%s] changelog write failed`, str)
	}

	str.cache.Delete(keyByt)

	return nil
}
