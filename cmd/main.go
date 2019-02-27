package main

import (
	"fmt"

	"github.com/MBtech/gpa-go"
)

func computeTime(partitions []partitioning.Partition, relativeComputeCost float32) float32 {
	maxPartitionSize := 0
	for i := range partitions {
		if len(partitions[i].Edges) > maxPartitionSize {
			maxPartitionSize = len(partitions[i].Edges)
		}
	}
	return float32(maxPartitionSize) * relativeComputeCost
}

func communicationTime(partitions []partitioning.Partition, relativeCommCost float32) float32 {
	maxCut := 0
	for i := range partitions {
		for j := i; j < len(partitions); j++ {
			cut := partitions[i].Vertices.Intersect(partitions[j].Vertices).Cardinality()
			if cut > maxCut {
				maxCut = cut
			}
		}
	}
	return float32(maxCut) * relativeCommCost
}

// Per iteration execution time
func executionTime(partitions []partitioning.Partition) float32 {
	cTime := computeTime(partitions, 0.1)
	commTime := communicationTime(partitions, 0.9)
	return cTime + commTime
}

func main() {
	numPartitions := 8
	// partitions := partitioning.Partitioner("com-lj.ungraph.txt", "\t", numPartitions)

	partitions := partitioning.Partitioner("testgraph.txt", " ", numPartitions)

	// fmt.Println(partitions)
	fmt.Println(executionTime(partitions))
}
