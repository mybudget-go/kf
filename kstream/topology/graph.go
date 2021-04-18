package topology

import (
	"fmt"
	"github.com/awalterschulze/gographviz"
	"strings"
)

type Visualizer interface {
	AddTopology(node Topology)
	Visualize() (string, error)
}

type graphViz struct {
	topology Topology
	graph    *gographviz.Graph
}

func NewTopologyVisualizer() Visualizer {

	parent := `root`
	g := gographviz.NewGraph()
	if err := g.SetName(parent); err != nil {
		panic(err)
	}
	if err := g.SetDir(true); err != nil {
		panic(err)
	}

	if err := g.AddAttr(parent, `splines`, `true`); err != nil {
		panic(err)
	}
	if err := g.AddAttr(parent, `size`, `"50,12"`); err != nil {
		panic(err)
	}

	if err := g.AddNode(parent, `kstreams`, map[string]string{
		`fontcolor`: `grey100`,
		`fillcolor`: `limegreen`,
		`style`:     `filled`,
		`label`:     `"KStreams"`,
	}); err != nil {
		panic(err)
	}

	return &graphViz{
		graph: g,
	}
}

func (g *graphViz) AddTopology(topology Topology) {
	g.topology = topology
}

func (g *graphViz) Visualize() (string, error) {
	for _, sbTp := range g.topology.SubTopologies() {
		// add sub graph(cluster) into graph
		sbId := fmt.Sprintf(`cluster_%d`, sbTp.Id().Id())
		if err := g.graph.AddSubGraph(`root`, sbId, map[string]string{
			`color`: `blue`,
			`label`: fmt.Sprintf(`"Sub-topology: %d"`, sbTp.Id().Id()),
		}); err != nil {
			return "", err
		}

		//add sources in the main graph
		for _, source := range sbTp.Sources() {
			topic := fmt.Sprintf(`"%s"`, source.Type().Attrs[`topic`])
			if err := g.graph.AddNode(`root`, topic, map[string]string{
				`label`: fmt.Sprintf(`"%s"`, source.Type().Attrs[`topic`]),
			}); err != nil {
				return ``, err
			}

			// add edges
			node := fmt.Sprintf(`"%d_%s"`, sbTp.Id().Id(), source.Id())
			if err := g.graph.AddEdge(topic, node, true, nil); err != nil {
				return ``, err
			}
		}

		for _, node := range sbTp.Nodes() {
			nId := fmt.Sprintf(`"%d_%s"`, sbTp.Id().Id(), node.Id())
			if err := g.graph.AddNode(sbId, nId, map[string]string{
				`label`: fmt.Sprintf(`"%s\n%s"`, strings.ToUpper(node.Type().Name), node.Id()),
			}); err != nil {
				return ``, err
			}

			// add sink edge
			if sink, ok := node.(Sink); ok {
				nId := fmt.Sprintf(`"%d_%s"`, sbTp.Id().Id(), node.Id())
				topic := fmt.Sprintf(`"%s"`, sink.Type().Attrs[`topic`])
				if err := g.graph.AddEdge(nId, topic, true, nil); err != nil {
					return ``, err
				}
			}
		}

		for _, edge := range sbTp.Edges() {
			parent := fmt.Sprintf(`"%d_%s"`, sbTp.Id().Id(), edge.Parent().Id())
			node := fmt.Sprintf(`"%d_%s"`, sbTp.Id().Id(), edge.Node().Id())
			if err := g.graph.AddEdge(parent, node, true, nil); err != nil {
				return ``, err
			}
		}
	}

	return g.graph.String(), nil
}
