package topology

import (
	"github.com/tryfix/kstream/kstream/serdes"
)

type RawRecord interface {
	Key() []byte
	Val() []byte
}

type SourceDeserializer struct {
	Key, Value serdes.Deserializer
}

type Source interface {
	Node
	NodeBuilder
	//WriteBuffer() chan <- RawRecord
	Deserializer() SourceDeserializer

	// Start opens the source WriteBuffer channel
	//Start() error

	// drain the recode channel and close
	//Close() error
}
