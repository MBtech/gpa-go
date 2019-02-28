package simulator

import (
	"math"

	"github.com/MBtech/gpa-go"
)

func computeTime(partitions []partitioning.Partition, relativeComputeCost float32, placement map[int]int) float32 {
	maxPartitionSize := float32(0.0)
	for i := range partitions {
		if float32(len(partitions[i].Edges))/float32(placement[i]) > maxPartitionSize {
			maxPartitionSize = float32(len(partitions[i].Edges)) / float32(placement[i])
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
func executionTime(partitions []partitioning.Partition, compute, comm float32, placement map[int]int) float32 {
	cTime := computeTime(partitions, compute, placement)
	commTime := communicationTime(partitions, comm)
	return cTime + commTime
}

// Should move these to a  helper package
func MinSize(partitions []partitioning.Partition) int {
	m := len(partitions[0].Edges)
	for _, v := range partitions {
		if len(v.Edges) < m {
			m = len(v.Edges)
		}
	}
	return m
}

//Should move these to a helper package
func MaxSize(partitions []partitioning.Partition) int {
	m := len(partitions[0].Edges)
	for _, v := range partitions {
		if len(v.Edges) > m {
			m = len(v.Edges)
		}
	}
	return m
}

func createCluster(partitions []partitioning.Partition) map[int]int {
	sizes := []int{1, 2, 4, 8}
	var placement map[int]int
	var load map[int]float64
	mx := MaxSize(partitions)
	mn := MinSize(partitions)
	for i := range partitions {
		load[i] = math.Ceil(float64(mx) / float64(mn))
	}

	for i := range load {
		placement[i] = int(math.Exp2(math.Round(math.Log2(load[i]))))
		if placement[i] > sizes[len(sizes)-1] {
			placement[i] = sizes[len(sizes)-1]
		}
	}

	return placement
}

func Sim(partitions []partitioning.Partition, compute, comm float32, clusterType string) float32 {
	var placement map[int]int
	if clusterType == "heterogeneous" {
		placement = createCluster(partitions)
	} else {
		for i := range partitions {
			placement[i] = 1
		}
	}

	return executionTime(partitions, compute, comm, placement)
}
