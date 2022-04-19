package tasks

import (
	"encoding/base64"
	"fmt"
	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/streams/topology"
	"sort"
	"strings"
)

type TaskAssignment []*TaskMapping

type TaskMapping struct {
	hash     string
	id       TaskID
	topology topology.SubTopologyBuilder
	TPs      []kafka.TopicPartition
}

func (a TaskMapping) TaskId() TaskID {
	return a.id
}

func (a TaskMapping) SubTopologyBuilder() topology.SubTopologyBuilder {
	return a.topology
}

func (a TaskAssignment) FindMappingByTP(partition kafka.TopicPartition) *TaskMapping {
	for _, mp := range a {
		for _, tp := range mp.TPs {
			if tp.String() == partition.String() {
				return mp
			}
		}
	}

	return nil
}

func (a *TaskAssignment) FindMappingsByTPs(partitions ...kafka.TopicPartition) TaskAssignment {
	var mappings []*TaskMapping
	added := map[TaskID]struct{}{}
	for _, partition := range partitions {
		for _, mp := range *a {
			for _, tp := range mp.TPs {
				if _, ok := added[mp.id]; !ok && tp.String() == partition.String() {
					mappings = append(mappings, mp)
					added[mp.id] = struct{}{}
				}
			}
		}
	}

	return mappings
}

func (a TaskAssignment) String() string {
	var prnt string
	for _, mp := range a {
		prnt += fmt.Sprintf("%s - %s \n", mp.id, mp.TPs)
	}

	return prnt
}

type Assigner struct{}

// Generate generates the Task Assignment for a given TopicPartition combination by assigning
// partitions to SubTopologyBuilders.
func (a *Assigner) Generate(tps []kafka.TopicPartition, topologyBuilder topology.Topology) TaskAssignment {
	// Create a map of sub topologies -> SubTopologyId:SubTopologyBuilder
	subTopologies := make(map[topology.SubTopologyId]topology.SubTopologyBuilder)
	for _, subTp := range topologyBuilder.StreamTopologies() {
		subTopologies[subTp.Id()] = subTp
	}

	// Create a topic:SubTopologyBuilder mapping
	topicsGroupedBySubTopology := map[topology.SubTopologyId]map[string]bool{}
	for _, subTopology := range subTopologies {
		topics := map[string]bool{}
		for _, src := range subTopology.Sources() {
			topics[src.Topic()] = true
		}
		topicsGroupedBySubTopology[subTopology.Id()] = topics
	}

	// Each SubTopologyBuilder has to be mapped to one or many topics and a single partition
	//
	//              			  +---- Topic 1 P0
	//              			  |
	//    Task 1(SubTopology1) ---|
	//              			  +---- Topic 2 P0
	//
	//
	//
	//              			  +---- Topic 1 P1
	//    Task 2(SubTopology2) ---|
	//              			  |
	//              			  +---- Topic 2 P1
	type topologyToTopicPartitionMap struct {
		hash          string
		subTopologyId topology.SubTopologyId
		partition     int32
		topics        []string
	}

	subTopologyToPartition := map[string]*topologyToTopicPartitionMap{}
	for _, tp := range tps {
		for subTopologyId, topics := range topicsGroupedBySubTopology {
			if _, ok := topics[tp.Topic]; ok {
				mapping, ok := subTopologyToPartition[fmt.Sprintf(`%s-%d`, subTopologyId.Name(), tp.Partition)]
				if !ok {
					subTopologyToPartition[fmt.Sprintf(`%s-%d`, subTopologyId.Name(), tp.Partition)] = &topologyToTopicPartitionMap{
						subTopologyId: subTopologyId,
						partition:     tp.Partition,
						topics:        []string{tp.Topic},
					}
					continue
				}

				mapping.topics = append(mapping.topics, tp.Topic)
			}
		}
	}

	// Assign a hash to each SubTopologyBuilder mapping. The hash has to be unique and consistent as this will be used
	// in each PartitionsAssigned event to figure out the Task assignment
	for _, mp := range subTopologyToPartition {
		sort.Strings(mp.topics)
		mp.hash = base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf(`%s#%d`, strings.Join(mp.topics, ``), mp.partition)))
	}

	var mappings []*TaskMapping

	// clean and map assignmentTpToTask to tasks
	for _, mp := range subTopologyToPartition {
		mapping := &TaskMapping{
			hash:     mp.hash,
			topology: topologyBuilder.SubTopology(mp.subTopologyId),
		}
		for _, tp := range mp.topics {
			mapping.TPs = append(mapping.TPs, kafka.TopicPartition{
				Topic:     tp,
				Partition: mp.partition,
			})
		}
		mappings = append(mappings, mapping)
	}

	// Sort tasks by assigned hash
	sort.Slice(mappings, func(i, j int) bool {
		return mappings[i].hash < mappings[j].hash
	})

	// Assign task Ids
	for i, mp := range mappings {
		mp.id = &taskId{
			id:        i,
			hash:      mp.hash,
			prefix:    "Task",
			partition: mp.TPs[0].Partition,
		}
	}

	return mappings
}
