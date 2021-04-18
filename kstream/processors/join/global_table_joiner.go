package join

import (
	"context"
	"fmt"
	"github.com/tryfix/errors"
	"github.com/tryfix/kstream/kstream/store"
	"github.com/tryfix/kstream/kstream/topology"
)

type GlobalTableJoiner struct {
	NId         topology.NodeId
	Typ         Type
	Store       string
	KeyMapper   KeyMapper
	ValueMapper ValueMapper
	store       store.Store
	Registry    store.Registry
	edges       []topology.Node
}

func (j *GlobalTableJoiner) SetId(id topology.NodeId) {
	j.NId = id
}

func (j *GlobalTableJoiner) Type() topology.Type {
	return topology.Type{
		Name: "GTableJoiner",
		Attrs: map[string]string{
			`store`: j.Store,
		},
	}
}

func (j *GlobalTableJoiner) AddEdge(node topology.Node) {
	j.edges = append(j.edges, node)
}

func (j *GlobalTableJoiner) Edges() []topology.Node {
	return j.edges
}

func (j *GlobalTableJoiner) Id() topology.NodeId {
	return j.NId
}

func (j *GlobalTableJoiner) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, next bool, err error) {
	joined, err := j.Join(ctx, kIn, vIn)
	if err != nil {
		return
	}

	for _, child := range j.edges {
		_, _, next, err := child.Run(ctx, kIn, joined)
		if err != nil || !next {
			return nil, nil, false, err
		}
	}

	return kIn, joined, true, err
}

func (j *GlobalTableJoiner) Build() (topology.Node, error) { //TODO: write new build
	s, err := j.Registry.Store(j.Store)
	if err != nil || s == nil {
		return nil, errors.New(`store [` + j.Store + `] dose not exist`)
	}
	j.store = s

	return &GlobalTableJoiner{
		NId:         j.Id(),
		Typ:         j.Typ,
		Store:       j.Store,
		KeyMapper:   j.KeyMapper,
		ValueMapper: j.ValueMapper,
		store:       j.store,
		Registry:    j.Registry,
	}, nil
}

func (j *GlobalTableJoiner) Join(ctx context.Context, key, leftVal interface{}) (joinedVal interface{}, err error) {

	// get key from key mapper
	k, err := j.KeyMapper(key, leftVal)
	if err != nil {
		return nil, errors.WithPrevious(err, `KeyMapper error`)
	}

	// get value from store
	rightValue, err := j.store.Get(ctx, k)
	if err != nil {
		return nil, errors.WithPrevious(err,
			fmt.Sprintf(`cannot get value from [%s] store`, j.Store))
	}

	// for InnerJoin joins if right side lookup nil ignore the join
	if j.Typ == InnerJoin && rightValue == nil {
		return nil, errors.New(
			fmt.Sprintf(`right value lookup failed due to [key [%+v] dose not exist in %s store]`, k, j.store.Name()))
	}

	// send LeftJoin value and right value to ValueJoiner and get the joined value
	valJoined, err := j.ValueMapper(leftVal, rightValue)
	if err != nil {
		return nil, errors.WithPrevious(err,
			`value mapper failed`)
	}

	return valJoined, nil

}

func (j *GlobalTableJoiner) Name() string {
	return j.Store
}
