package stores

import "github.com/gmbyapa/kstream/backend"

type keyVal struct {
	key, val interface{}
}

type indexIterator struct {
	records    []*keyVal
	currentKey int
	valid      bool
}

func (i *indexIterator) SeekToFirst() {
	i.valid = len(i.records) > 0
	i.currentKey = 0
}

func (i *indexIterator) SeekToLast() {
	i.valid = len(i.records) > 0
	i.currentKey = len(i.records) - 1
}

func (i *indexIterator) Seek(key interface{}) error {
	i.valid = len(i.records) > 0
	for idx, r := range i.records {
		if r.key == key {
			i.currentKey = idx
		}
	}

	return nil
}

func (i *indexIterator) Next() {
	if i.currentKey == len(i.records)-1 {
		i.valid = false
		return
	}
	i.currentKey += 1
}

func (i *indexIterator) Prev() {
	if i.currentKey < 0 {
		i.valid = false
		return
	}
	i.currentKey += 1
}

func (i *indexIterator) Close() {
	i.records = nil
}

func (i *indexIterator) Key() (interface{}, error) {
	return i.records[i.currentKey].key, nil
}

func (i *indexIterator) Value() (interface{}, error) {
	return i.records[i.currentKey].val, nil
}

func (i *indexIterator) Valid() bool {
	return i.valid
}

func (i *indexIterator) Error() error {
	return nil
}

func (i *indexIterator) BackendIterator() backend.Iterator {
	panic(``)
}
