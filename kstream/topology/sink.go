package topology

import (
	"github.com/tryfix/kstream/kstream/serdes"
)

type SinkSerializer struct {
	Key, Value serdes.Serializer
}

type Sink interface {
	Node
	NodeBuilder
	Serializer() SinkSerializer

	// close the source buffers
	Close() error
}
