package topology

type SubTopology interface {
	Id() SubTopologyId
	Sinks() []Sink
	Source(topic string) Source
	Nodes() []Node
	Init(ctx SubTopologyContext) error
	Destroy() error
	Store(name string) StateStore
	StateStores() map[string]StateStore
}
