package tasks

import (
	"github.com/gmbyapa/kstream/kafka/mocks"
	logger "github.com/tryfix/log"
	"testing"
	"time"
)

var bufferConf = BufferConfig{
	Size:          10,
	FlushInterval: 1 * time.Second,
}

func TestBuffer_Add(t *testing.T) {
	var i int
	b := newBuffer(bufferConf, func(records []*Record) error {
		i = len(records)
		return nil
	}, logger.NewNoopLogger())

	for i := 0; i < 10; i++ {
		if err := b.Add(&Record{Record: &mocks.Record{}}); err != nil {
			t.Error(err)
		}
	}

	time.Sleep(2 * time.Second)

	if i != 10 {
		t.Fail()
	}
}

func TestBuffer_Flush(t *testing.T) {
	var i int
	b := newBuffer(bufferConf, func(records []*Record) error {
		i = len(records)
		return nil
	}, logger.NewNoopLogger())

	for i := 0; i < 10; i++ {
		if err := b.Add(&Record{Record: &mocks.Record{}}); err != nil {
			t.Error(err)
		}
	}

	if err := b.Flush(); err != nil {
		t.Error(err)
	}

	if i != 10 {
		t.Fail()
	}
}

func TestBuffer_Flush_When_Closing(t *testing.T) {
	var i int
	b := newBuffer(bufferConf, func(records []*Record) error {
		i = len(records)
		return nil
	}, logger.NewNoopLogger())

	for i := 0; i < 10; i++ {
		if err := b.Add(&Record{Record: &mocks.Record{}}); err != nil {
			t.Error(err)
		}
	}

	if err := b.Close(); err != nil {
		t.Error(err)
	}

	if i != 10 {
		t.Fail()
	}
}
