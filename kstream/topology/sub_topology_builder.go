package topology

type SubTopologyId struct {
	id   int
	name string
}

func (id SubTopologyId) Id() int {
	return id.id
}

type SubTopologyBuilder interface {
	Id() SubTopologyId
	SetId(SubTopologyId)
	AddNode(builder NodeBuilder)
	AddNodeWithEdge(node, edge NodeBuilder)
	Build() (SubTopology, error)
	AddEdge(parent, node NodeBuilder)
	Edges() []Edge
	Sources() []Source
	AddSource(source Source)
}

type Edge struct {
	parent, node NodeBuilder
	edges        []NodeBuilder
}

func (e Edge) Parent() NodeBuilder {
	return e.parent
}

func (e Edge) Node() NodeBuilder {
	return e.node
}

type KSubTopologyBuilder struct {
	id           SubTopologyId
	nodeBuilders []NodeBuilder
	sources      []Source
	sinks        []Sink
	edges        []Edge
}

func (b *KSubTopologyBuilder) Id() SubTopologyId {
	return b.id
}

func (b *KSubTopologyBuilder) SetId(id SubTopologyId) {
	b.id = id
}

func (b *KSubTopologyBuilder) AddNode(node NodeBuilder) {
	node.SetId(NewNodeId(len(b.nodeBuilders), `test.path`))
	b.nodeBuilders = append(b.nodeBuilders, node)

	// if node is a sink append to the sink list
	if snk, ok := node.(Sink); ok {
		b.sinks = append(b.sinks, snk)
	}
}

func (b *KSubTopologyBuilder) AddNodeWithEdge(node, edge NodeBuilder) {
	b.AddNode(edge)
	b.AddEdge(node, edge)
}

// AddEdge adds an Edge to the topology
// Note: this method can create cyclic call dependencies
func (b *KSubTopologyBuilder) AddEdge(parent, node NodeBuilder) {
	// TODO check cyclic dependencies
	b.edges = append(b.edges, Edge{parent: parent, node: node})
}

func (b *KSubTopologyBuilder) Edges() []Edge {
	return b.edges
}

func (b *KSubTopologyBuilder) Sources() []Source {
	return b.sources
}

func (b *KSubTopologyBuilder) AddSource(source Source) {
	b.AddNode(source)
	b.sources = append(b.sources, source)
}

func (b *KSubTopologyBuilder) Build() (SubTopology, error) {
	// build nodeBuilders
	nds := make([]Node, len(b.nodeBuilders))
	for i, node := range b.nodeBuilders {
		nd, err := node.Build()
		if err != nil {
			return nil, err
		}
		nds[i] = nd
	}

	// connect Edges
	for _, edge := range b.Edges() {
		// find parent and add Edge
		nds[edge.parent.Id().id].AddEdge(nds[edge.node.Id().id])
	}

	return &KSubTopology{
		root:    nds[0],
		id:      b.id,
		edges:   b.edges,
		nodes:   nds,
		sources: b.sources,
		sinks:   b.sinks,
	}, nil
}
