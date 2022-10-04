/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package badger

import (
	"fmt"
	db "github.com/dgraph-io/badger/v3"
)

type Iterator struct {
	itr    *db.Iterator
	closed chan struct{}
}

func (i *Iterator) SeekToFirst() {
	i.itr.Rewind()
}

func (i *Iterator) SeekToLast() {}

func (i *Iterator) Seek(key []byte) {
	i.itr.Seek(key)
}

func (i *Iterator) Next() {
	i.itr.Next()
}

func (i *Iterator) Prev() {

}

func (i *Iterator) Close() {
	i.itr.Close()
	close(i.closed)
}

func (i *Iterator) Key() []byte {
	return i.itr.Item().Key()
}

func (i *Iterator) Value() []byte {
	val, err := i.itr.Item().ValueCopy(nil)
	if err != nil {
		panic(fmt.Sprintf(`iterator error: %s`, err))
	}

	return val
}

func (i *Iterator) Valid() bool {
	return i.itr.Valid()
}

func (i *Iterator) Error() error {
	return nil
}
