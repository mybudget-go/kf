/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package backend

import (
	"time"
)

type Builder func(name string) (Backend, error)

type Backend interface {
	// Name returns the name of the store
	Name() string
	// Set writes the given key:value pair with an optional expiry
	Set(key []byte, value []byte, expiry time.Duration) error
	// Get looks for the value of a given key. Will return nil if the value does not exist
	Get(key []byte) ([]byte, error)

	PrefixedIterator(keyPrefix []byte) Iterator
	Iterator() Iterator
	Delete(key []byte) error
	String() string
	Persistent() bool
	Close() error
}
