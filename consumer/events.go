/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package consumer

import (
	"fmt"
)

type Event interface {
	String() string
}

type PartitionEnd struct {
	Tps []TopicPartition
}

func (p *PartitionEnd) String() string {
	return fmt.Sprintf(`%v`, p.Tps)
}

func (p *PartitionEnd) TopicPartitions() []TopicPartition {
	return p.Tps
}
