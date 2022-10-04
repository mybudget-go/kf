package state_stores

import (
	"bytes"
	"testing"
)

func TestCache_Delete(t *testing.T) {
	cache := newCache()
	cache.records = map[string][]byte{
		`test`: []byte(`test`),
	}

	cache.Delete([]byte(`test`))
	if v := cache.records[`test`]; v != nil {
		t.Fail()
	}

	if !cache.Has([]byte(`test`)) {
		t.Fail()
	}
}

func TestCache_Has(t *testing.T) {
	type fields struct {
		records map[string][]byte
	}
	type args struct {
		key []byte
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name:   "exists",
			fields: fields{map[string][]byte{`test`: []byte(`test`)}},
			args:   args{[]byte(`test`)},
			want:   true,
		},
		{
			name:   "does_not_exists",
			fields: fields{map[string][]byte{`test`: []byte(`test`)}},
			args:   args{[]byte(`unknown`)},
			want:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Cache{
				records: tt.fields.records,
			}
			if got := c.Has(tt.args.key); got != tt.want {
				t.Errorf("Has() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCache_Purge(t *testing.T) {
	type fields struct {
		records map[string][]byte
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name:   "purge_all",
			fields: fields{map[string][]byte{`test`: []byte(`test`)}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Cache{
				records: tt.fields.records,
			}
			c.Purge()
			if len(c.records) > 0 {
				t.Fail()
			}
		})
	}
}

func TestCache_Read(t *testing.T) {
	cache := newCache()
	cache.records = map[string][]byte{
		`test`: []byte(`test`),
	}

	key := cache.Read([]byte(`test`))
	if !bytes.Equal(key, []byte(`test`)) {
		t.Fail()
	}
}

func TestCache_Write(t *testing.T) {
	cache := newCache()
	cache.records = map[string][]byte{}

	cache.Write([]byte(`test`), []byte(`test`))
	if !bytes.Equal(cache.Read([]byte(`test`)), []byte(`test`)) {
		t.Fail()
	}
}
