package main

import (
	"fmt"

	"github.com/MBtech/gpa-go"
	"github.com/MBtech/gpa-go/plots"
	"github.com/MBtech/gpa-go/simulator"
	"github.com/deckarep/golang-set"
)

func main() {
	listPartitions := []int{2, 4, 8, 16}
	// graphfile := "com-lj.ungraph.txt"
	// graphfile := "roadNet-TX.txt"
	graphfile := "wiki-topcats.txt"

	// numPartitions := 4
	simulation := false
	algorithms := []string{"hdrf", "eti", "hash"}
	repFactor := make(map[string][]float64)
	imbl := make(map[string][]float64)
	mxMn := make(map[string][]float64)
	for _, algo := range algorithms {
		repFactor[algo] = []float64{}
		for _, numPartitions := range listPartitions {
			// algo := "hdrf"

			// partitions := partitioning.Partitioner("com-lj.ungraph.txt", "\t", numPartitions, algo)

			partitions := partitioning.Partitioner(graphfile, " ", numPartitions, algo)

			// Replica factor while changing number of partitions
			// Average number of replicas per vertex
			// divide total number of vertices by the actual number of vertices
			totalV := 0
			RealV := mapset.NewSet()
			totalE := 0
			for _, part := range partitions {
				// fmt.Println(part.Vertices.Cardinality())
				RealV = part.Vertices.Union(RealV)
				totalE += len(part.Edges)
				totalV += part.Vertices.Cardinality()
			}
			// fmt.Println(totalV)
			// fmt.Println(RealV.Cardinality())
			repFactor[algo] = append(repFactor[algo], float64(totalV)/float64(RealV.Cardinality()))

			//load imbalance w.r.t. full balance
			imbl[algo] = append(imbl[algo], float64(simulator.MaxSize(partitions))/(float64(totalE)/float64(numPartitions)))

			// Load imbalance max-min ratio of load
			// edge wise and vertex wise
			mxMn[algo] = append(mxMn[algo], float64(simulator.MaxSize(partitions))/(0.001+float64(simulator.MinSize(partitions))))

			// fmt.Println(partitions)
			if simulation {
				compute := float32(0.1)
				comm := float32(0.9)
				clusterType := "normal"
				// clusterType := "heterogeneous"
				fmt.Println(simulator.Sim(partitions, compute, comm, clusterType))
			}
		}
	}

	plots.Plt(repFactor, listPartitions, "Replication-Factor-"+graphfile, "Partitions", "Replication Factor")
	plots.Plt(imbl, listPartitions, "Imbl-perfect-"+graphfile, "Partitions", "MaxLoad/PerfectLoad")
	plots.Plt(mxMn, listPartitions, "Imbl-MaxMin-"+graphfile, "Partitions", "MaxLoad/MinLoad")
}
