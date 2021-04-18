package topology

type TopologyBuilder interface {
	AddSubTopology(builder SubTopologyBuilder)
	SubTopologies() []SubTopologyBuilder
	Build() (Topology, error)
	//Describe() string
}

type KTopologyBuilder struct {
	subTopologies []SubTopologyBuilder
}

func (k *KTopologyBuilder) AddSubTopology(builder SubTopologyBuilder) {
	k.subTopologies = append(k.subTopologies, builder)
	builder.SetId(SubTopologyId{
		id:   len(k.subTopologies),
		name: "",
	})
}

func (k *KTopologyBuilder) SubTopologies() []SubTopologyBuilder {
	return k.subTopologies
}

func (k *KTopologyBuilder) Build() (Topology, error) {
	kTp := &kTopology{streams: []SubTopology{}}
	for _, st := range k.subTopologies {
		stp, err := st.Build()
		if err != nil {
			return nil, err
		}
		kTp.streams = append(kTp.streams, stp)
	}

	return kTp, nil
}
