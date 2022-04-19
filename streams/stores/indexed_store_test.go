package stores

import (
	"context"
	"github.com/gmbyapa/kstream/backend"
	"github.com/gmbyapa/kstream/streams/encoding"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func Test_indexedStore_Delete(t *testing.T) {
	index := NewIndex(`foo`, func(key, val interface{}) (idx interface{}) {
		return strings.Split(val.(string), `,`)[0]
	})

	i := &indexedStore{
		Store:   NewMockStore(`foo`, encoding.StringEncoder{}, encoding.StringEncoder{}, backend.NewMockBackend(`foo`, 0)),
		indexes: map[string]Index{`foo`: index},
		mu:      new(sync.Mutex),
	}

	if err := i.Set(context.Background(), `200`, `111,222`, 0); err != nil {
		t.Error(err)
	}

	if err := i.Set(context.Background(), `300`, `111,333`, 0); err != nil {
		t.Error(err)
	}

	if err := i.Delete(context.Background(), `200`); err != nil {
		t.Error(err)
	}

	data, err := index.Read(`111`)
	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(data, []interface{}{`300`}) {
		t.Errorf(`want []string{300}, have %#v`, data)
	}
}

func Test_indexedStore_Set(t *testing.T) {
	index := NewStringHashIndex(`foo`, func(key, val interface{}) (idx string) {
		return strings.Split(val.(string), `,`)[0]
	})

	i := &indexedStore{
		Store:   NewMockStore(`foo`, encoding.StringEncoder{}, encoding.StringEncoder{}, backend.NewMockBackend(`foo`, 0)),
		indexes: map[string]Index{`foo`: index},
		mu:      new(sync.Mutex),
	}

	if err := i.Set(context.Background(), `200`, `111,222`, 0); err != nil {
		t.Error(err)
	}

	if err := i.Set(context.Background(), `300`, `111,333`, 0); err != nil {
		t.Error(err)
	}

	data, err := index.Read(`111`)
	if err != nil {
		t.Error(data)
	}

	var want []interface{}
	for _, r := range data {
		if r.(string) == `200` || r.(string) == `300` {
			want = append(want, r)
		}
	}

	if len(want) < 2 {
		t.Fail()
	}
}

func TestIndexedStore_GetIndexedRecords(t *testing.T) {
	index := NewStringHashIndex(`foo`, func(key, val interface{}) (idx string) {
		return strings.Split(val.(string), `,`)[0]
	})

	i := &indexedStore{
		Store:   NewMockStore(`foo`, encoding.StringEncoder{}, encoding.StringEncoder{}, backend.NewMockBackend(`foo`, 0)),
		indexes: map[string]Index{`foo`: index},
		mu:      new(sync.Mutex),
	}

	if err := i.Set(context.Background(), `200`, `111,222`, 0); err != nil {
		t.Error(err)
	}
	if err := i.Set(context.Background(), `300`, `111,333`, 0); err != nil {
		t.Error(err)
	}
	if err := i.Set(context.Background(), `400`, `222,333`, 0); err != nil {
		t.Error(err)
	}

	itr, err := i.GetIndexedRecords(context.Background(), `foo`, `111`)
	if err != nil {
		t.Error(err)
	}

	var want []interface{}
	for itr.SeekToFirst(); itr.Valid(); itr.Next() {
		val, err := itr.Value()
		if err != nil {
			t.Error(err)
		}

		if val.(string) == `111,222` || val.(string) == `111,333` {
			want = append(want, val)
		}
	}

	if len(want) < 2 {
		t.Fail()
	}
}
