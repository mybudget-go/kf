package topology

type Topology interface {
	SubTopologies() []SubTopologyBuilder
	SubTopology(id SubTopologyId) SubTopologyBuilder
	SubTopologyByTopic(topic string) SubTopologyBuilder
	StreamTopologies() SubTopologyBuilders
	GlobalTableTopologies() SubTopologyBuilders
	Describe() string
}
