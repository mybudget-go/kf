package topology

import (
	"context"
	"fmt"
)

type NodeId struct {
	id   int
	path string
}

func NewNodeId(id int, path string) NodeId {
	return NodeId{
		id:   id,
		path: path,
	}
}

func (id NodeId) String() string {
	return fmt.Sprintf(`%05d`, id.id+1)
}

type NodeInfo interface {
	Id() NodeId
	SetId(id NodeId)
	Type() Type
}

type NodeBuilder interface {
	NodeInfo
	Build() (Node, error)
	//AddEdge(node NodeBuilder)
}

type Type struct {
	Name  string
	Attrs map[string]string
}

type Node interface {
	NodeInfo
	Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error)
	AddEdge(node Node)
	Edges() []Node
}
