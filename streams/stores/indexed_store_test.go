package stores

import (
	"context"
	"github.com/gmbyapa/kstream/backend"
	"github.com/gmbyapa/kstream/streams/encoding"
	"reflect"
	"strings"
	"testing"
)

func indexStoreTestStoreSetup(t *testing.T, idx Index) IndexedStore {
	i, err := NewIndexedStore(
		`foo`,
		encoding.StringEncoder{},
		encoding.StringEncoder{},
		[]Index{idx},
		WithBackend(backend.NewMockBackend(`foo`, 0)),
	)
	if err != nil {
		t.Error(err)
	}

	return i
}

func indexStoreIndexSetup(t *testing.T) Index {
	return NewIndex(`foo`, func(key, val interface{}) (idx interface{}) {
		return strings.Split(val.(string), `,`)[0]
	})
}

func Test_indexedStore_Delete(t *testing.T) {
	idx := indexStoreIndexSetup(t)
	str := indexStoreTestStoreSetup(t, idx)

	if err := str.Set(context.Background(), `200`, `111,222`, 0); err != nil {
		t.Error(err)
	}

	if err := str.Set(context.Background(), `300`, `111,333`, 0); err != nil {
		t.Error(err)
	}

	if err := str.Delete(context.Background(), `200`); err != nil {
		t.Error(err)
	}

	data, err := idx.Read(`111`)
	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(data, []interface{}{`300`}) {
		t.Errorf(`want []string{300}, have %#v`, data)
	}
}

func Test_indexedStore_Delete_Should_Remove_Indexed_Values(t *testing.T) {
	idx := indexStoreIndexSetup(t)
	str := indexStoreTestStoreSetup(t, idx)

	if err := str.Set(context.Background(), `200`, `111,222`, 0); err != nil {
		t.Error(err)
	}

	if err := str.Set(context.Background(), `300`, `111,333`, 0); err != nil {
		t.Error(err)
	}

	if err := str.Delete(context.Background(), `200`); err != nil {
		t.Error(err)
	}

	if err := str.Delete(context.Background(), `300`); err != nil {
		t.Error(err)
	}

	data, err := idx.Read(`111`)
	if err != nil {
		t.Error(err)
	}

	if len(data) > 0 {
		t.Errorf(`want []string{300}, have %#v`, data)
	}
}

func Test_indexedStore_Set(t *testing.T) {
	idx := indexStoreIndexSetup(t)
	str := indexStoreTestStoreSetup(t, idx)

	if err := str.Set(context.Background(), `200`, `111,222`, 0); err != nil {
		t.Error(err)
	}

	if err := str.Set(context.Background(), `300`, `111,333`, 0); err != nil {
		t.Error(err)
	}

	data, err := idx.Read(`111`)
	if err != nil {
		t.Error(err)
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

func Test_indexedStore_Set_OldIndexedValues_ShouldGetDeleted(t *testing.T) {
	idx := indexStoreIndexSetup(t)
	str := indexStoreTestStoreSetup(t, idx)

	if err := str.Set(context.Background(), `200`, `111,222`, 0); err != nil {
		t.Error(err)
	}

	if err := str.Set(context.Background(), `200`, `222,333`, 0); err != nil {
		t.Error(err)
	}

	data, err := idx.Read(`111`)
	if err != nil {
		t.Error(err)
	}

	if len(data) > 0 {
		t.Fail()
	}
}

func TestIndexedStore_GetIndexedRecords(t *testing.T) {
	idx := indexStoreIndexSetup(t)
	str := indexStoreTestStoreSetup(t, idx)

	if err := str.Set(context.Background(), `200`, `111,222`, 0); err != nil {
		t.Error(err)
	}
	if err := str.Set(context.Background(), `300`, `111,333`, 0); err != nil {
		t.Error(err)
	}
	if err := str.Set(context.Background(), `400`, `222,333`, 0); err != nil {
		t.Error(err)
	}

	itr, err := str.GetIndexedRecords(context.Background(), `foo`, `111`)
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
