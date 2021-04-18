package streams

import (
	"context"
	"github.com/tryfix/kstream/kstream/serdes"
	"github.com/tryfix/kstream/kstream/topology"
)

type GTable struct {
	*topology.KSubTopologyBuilder
	*topology.KSubTopology
	source topology.Source
	serde  serdes.Deserializer
}

func NewGlobalTableTopology() topology.SubTopologyBuilder {
	tbl := &GTable{
		KSubTopologyBuilder: new(topology.KSubTopologyBuilder),
		serde:               serdes.ByteArraySerializer{},
	}

	src := NewKSource(`test`, topology.SourceDeserializer{
		Key:   tbl.serde,
		Value: tbl.serde,
	})

	tbl.AddNode(src)

	//store :=

	return nil
}

func (t *GTable) Run(ctx context.Context, kIn, vIn interface{}) error {
	if _, _, _, err := t.source.Run(ctx, kIn, vIn); err != nil {
		return err
	}

	return nil
}
