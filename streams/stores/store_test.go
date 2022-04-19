package stores_test

import (
	"context"
	"fmt"
	"github.com/gmbyapa/kstream/backend/memory"
	"github.com/gmbyapa/kstream/streams/encoding"
	"github.com/gmbyapa/kstream/streams/stores"
	"reflect"
	"sort"
	"testing"
	"time"
)

func makeTestStore(t *testing.T, expiry time.Duration) stores.Store {
	conf := memory.NewConfig()
	conf.RecordExpiryEnabled = true
	conf.RecordExpiry = expiry
	conf.ExpiredRecordCleanupInterval = 10 * time.Millisecond
	stor, err := stores.NewStore(
		`test_store`,
		encoding.StringEncoder{},
		encoding.StringEncoder{},
		stores.WithBackend(memory.NewMemoryBackend(`test_backend`, conf)))
	if err != nil {
		t.Error(err)
	}

	return stor
}

func TestDefaultStore_Get(t *testing.T) {
	ctx := context.Background()
	st := makeTestStore(t, 0)
	testValue := `test_value`
	err := st.Set(ctx, `100`, testValue, 0)
	if err != nil {
		t.Error(err)
		return
	}

	v, err := st.Get(ctx, `100`)
	if err != nil {
		t.Error(err)
	}

	if v != testValue {
		t.Fail()
	}
}

func TestDefaultStore_Get_Should_return_Nul_For_Invalid_Key(t *testing.T) {
	ctx := context.Background()
	st := makeTestStore(t, 0)
	testValue := `test_value`
	testKey := `100`
	err := st.Set(ctx, testKey, testValue, 0)
	if err != nil {
		t.Error(err)
		return
	}

	v, err := st.Get(ctx, `200`)
	if err != nil {
		t.Error(err)
	}

	if v != nil {
		t.Fail()
	}
}

func TestDefaultStore_Set(t *testing.T) {
	ctx := context.Background()
	st := makeTestStore(t, 0)
	testValue := `test_value`
	testKey := `100`
	err := st.Set(ctx, testKey, testValue, 0)
	if err != nil {
		t.Error(err)
		return
	}

	v, err := st.Get(ctx, testKey)
	if err != nil {
		t.Error(err)
	}

	if v != testValue {
		t.Fail()
	}
}

func TestDefaultStore_Delete(t *testing.T) {
	ctx := context.Background()
	st := makeTestStore(t, 0)
	testValue := `test_value`
	testKey := `100`
	err := st.Set(ctx, testKey, testValue, 0)
	if err != nil {
		t.Error(err)
		return
	}

	v, err := st.Get(ctx, testKey)
	if err != nil {
		t.Error(err)
	}

	if v != testValue {
		t.Fail()
	}
}

func TestDefaultStore_Set_Record_Expiry(t *testing.T) {
	ctx := context.Background()
	st := makeTestStore(t, 0)
	testValue := `test_value`
	testKey := `100`
	expiry := 100 * time.Millisecond
	err := st.Set(ctx, testKey, testValue, expiry)
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(expiry * 2)

	v, err := st.Get(ctx, testKey)
	if err != nil {
		t.Error(err)
	}

	if v != nil {
		t.Fail()
	}
}

func TestDefaultStore_Set_Store_Expiry(t *testing.T) {
	ctx := context.Background()
	expiry := 100 * time.Millisecond
	st := makeTestStore(t, expiry)
	testValue := `test_value`
	testKey := `100`
	err := st.Set(ctx, testKey, testValue, 0)
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(expiry * 2)

	v, err := st.Get(ctx, testKey)
	if err != nil {
		t.Error(err)
	}

	if v != nil {
		t.Fail()
	}
}

func TestDefaultStore_GetAll_FirstToLast(t *testing.T) {
	ctx := context.Background()
	st := makeTestStore(t, 0)

	var keyVals []string
	for i := 1; i <= 1000; i++ {
		key := fmt.Sprint(i)
		val := fmt.Sprintf(`%d`, i*10)
		keyVals = append(keyVals, fmt.Sprintf(`%s-%s`, key, val))
		if err := st.Set(ctx, key, val, 0); err != nil {
			t.Fatal(err)
		}
	}
	sort.Strings(keyVals)

	i, err := st.Iterator(ctx)
	if err != nil {
		t.Error(err)
	}
	defer i.Close()

	var keyValsHave []string
	for i.SeekToFirst(); i.Valid(); i.Next() {
		key, err := i.Key()
		if err != nil {
			t.Error(err)
		}

		val, err := i.Value()
		if err != nil {
			t.Error(err)
		}
		keyValsHave = append(keyValsHave, fmt.Sprintf(`%s-%s`, key.(string), val.(string)))
	}

	sort.Strings(keyValsHave)
	if !reflect.DeepEqual(keyVals, keyValsHave) {
		t.Fail()
	}
}

func TestDefaultStore_GetAll_LastToFirst(t *testing.T) {
	ctx := context.Background()
	st := makeTestStore(t, 0)

	var keyVals []string
	for i := 1; i <= 1000; i++ {
		key := fmt.Sprint(i)
		val := fmt.Sprintf(`%d`, i*10)
		keyVals = append(keyVals, fmt.Sprintf(`%s-%s`, key, val))
		if err := st.Set(ctx, key, val, 0); err != nil {
			t.Fatal(err)
		}
	}
	sort.Strings(keyVals)

	i, err := st.Iterator(ctx)
	if err != nil {
		t.Error(err)
	}
	defer i.Close()

	var keyValsHave []string
	for i.SeekToLast(); i.Valid(); i.Prev() {
		key, err := i.Key()
		if err != nil {
			t.Error(err)
		}

		val, err := i.Value()
		if err != nil {
			t.Error(err)
		}
		keyValsHave = append(keyValsHave, fmt.Sprintf(`%s-%s`, key.(string), val.(string)))
	}

	sort.Strings(keyValsHave)
	if !reflect.DeepEqual(keyVals, keyValsHave) {
		t.Fail()
	}
}
