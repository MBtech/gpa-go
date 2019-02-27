package partitioning

import (
	"fmt"
	"hash/fnv"
	"math"
	"sort"

	"github.com/deckarep/golang-set"
	"github.com/dgryski/go-metro"
	"github.com/dgryski/go-minhash"
	"github.com/dgryski/go-spooky"
)

// These should actually be moved to a separate file

// Partition is a structure containing the partition information
type Partition struct {
	Edges    []Edge
	Vertices mapset.Set
}

// Edge data structure that contains src and dest vertex ID and the value for the edge
type Edge struct {
	Src   int
	Dest  int
	Value int
}

type Tuple struct {
	partition1 int
	partition2 int
	similarity float64
}

var pDegree map[int]int
var membership map[int]mapset.Set
var partitionSizes map[int]int

// var totalpartitions = 10

func HashPartitioner(src, dest int, totalPart int) int {
	hsrc := fnv.New32a()
	hsrc.Write([]byte(string(src)))
	hdest := fnv.New32a()
	hdest.Write([]byte(string(dest)))
	p := (int(hsrc.Sum32())%totalPart + int(hdest.Sum32())%totalPart) % totalPart
	return p
}

func DBH(src, dest int, totalPart int) int {
	pdSrc := 0
	pdDest := 0
	if pDegree[src] == 0 {
		pDegree[src] = 1
	} else {
		pDegree[src] += 1
		pdSrc = pDegree[src]
	}
	if pDegree[dest] == 0 {
		pDegree[dest] = 1
	} else {
		pDegree[dest] += 1
		pdDest = pDegree[dest]
	}

	if pdSrc < pdDest {
		hsrc := fnv.New32a()
		hsrc.Write([]byte(string(src)))
		return int(hsrc.Sum32()) % totalPart
	} else {
		hdest := fnv.New32a()
		hdest.Write([]byte(string(dest)))
		return int(hdest.Sum32()) % totalPart
	}
	// return p
}

func GreedyPartition(src, dest int, totalPart int) int {
	if membership[src] == nil {
		membership[src] = mapset.NewSet()
	}
	if membership[dest] == nil {
		membership[dest] = mapset.NewSet()
	}
	intersect := membership[src].Intersect(membership[dest])
	union := membership[src].Union(membership[dest])
	if intersect.Cardinality() > 0 {
		pid := leastLoaded(intersect)
		partitionSizes[pid] += 1
		return pid
	}
	if intersect.Cardinality() == 0 && union.Cardinality() > 0 {
		pid := leastLoaded(union)
		partitionSizes[pid] += 1
		return pid
	}
	if membership[src].Cardinality() == 0 && membership[dest].Cardinality() > 0 {
		pid := leastLoaded(membership[dest])
		partitionSizes[pid] += 1
		return pid
	}
	if membership[src].Cardinality() > 0 && membership[dest].Cardinality() == 0 {
		pid := leastLoaded(membership[src])
		partitionSizes[pid] += 1
		return pid
	} else {
		pid := leastLoaded(nil)
		// This is the only case in which we need to update the membership
		membership[src].Add(pid)
		membership[dest].Add(pid)
		partitionSizes[pid] += 1
		return pid
	}

}

func mergePartitions(t Tuple, partitions []Partition) []Partition {
	partitions[t.partition1].Vertices = partitions[t.partition1].Vertices.Union(partitions[t.partition2].Vertices)
	partitions[t.partition1].Edges = append(partitions[t.partition1].Edges, partitions[t.partition2].Edges...)
	// partitions[].Vertices = mapset.NewSet()
	// partitions[j].Edges = []Edge{}
	partitions = append(partitions[:t.partition2], partitions[t.partition2+1:]...)
	return partitions
}

func HDRFPartition(src, dest int, totalPart int, partitions []Partition) int {
	pdSrc := 0
	pdDest := 0
	// Partial degree calculation
	if pDegree[src] == 0 {
		pDegree[src] = 1
	} else {
		pDegree[src] += 1
		pdSrc = pDegree[src]
	}
	if pDegree[dest] == 0 {
		pDegree[dest] = 1
	} else {
		pDegree[dest] += 1
		pdDest = pDegree[dest]
	}

	thetaSrc := float64(pdSrc) / float64((pdSrc + pdDest))
	thetaDest := 1 - thetaSrc
	N := len(partitions)
	cBal := make([]float64, N)
	cRep := make([]map[string]float64, N)
	cHDRF := make([]map[string]float64, N)
	l := 1.0
	ep := 0.001
	maxsize := maxSize(partitions)
	minsize := minSize(partitions)
	max := 0.0
	maxi := 0
	for i := 0; i < N; i++ {
		cBal[i] = l * float64(maxsize-len(partitions[i].Edges)) / (ep + float64(maxsize-minsize))
		if cRep[i] == nil {
			cRep[i] = make(map[string]float64)
		}
		if cHDRF[i] == nil {
			cHDRF[i] = make(map[string]float64)
		}

		cRep[i][string(src)+","+string(dest)] = g(src, i, thetaSrc, partitions) + g(dest, i, thetaDest, partitions)
		cHDRF[i][string(src)+","+string(dest)] = cRep[i][string(src)+","+string(dest)] + cBal[i]
		if cHDRF[i][string(src)+","+string(dest)] > max {
			max = cHDRF[i][string(src)+","+string(dest)]
			maxi = i
		}
	}

	return maxi
}

func g(v, i int, theta float64, partitions []Partition) float64 {
	if partitions[i].Vertices.Contains(v) {
		return 1.0 + (1.0 - theta)
	} else {
		return 0.0
	}
	// return 0.0
}
func minSize(partitions []Partition) int {
	m := len(partitions[0].Edges)
	for _, v := range partitions {
		if len(v.Edges) < m {
			m = len(v.Edges)
		}
	}
	return m
}

func maxSize(partitions []Partition) int {
	m := len(partitions[0].Edges)
	for _, v := range partitions {
		if len(v.Edges) > m {
			m = len(v.Edges)
		}
	}
	return m
}
func mhash(b []byte) uint64 { return metro.Hash64(b, 0) }

func ETIPartition(partitions []Partition) []Partition {
	// N := cap(partitions)
	// merge := make(map[int][]int)
	// pullMatrix := make(map[int][]float64)

	fmt.Println("Creating the pull matrix")

	sigs := []*minhash.MinWise{}

	for i := 0; i < len(partitions); i++ {
		m := minhash.NewMinWise(spooky.Hash64, mhash, 10)
		for v := range partitions[i].Vertices.Iterator().C {
			m.Push([]byte(string(v.(int))))
		}
		sigs = append(sigs, m)
	}

	check := true
	for check {
		pullMatrix := []Tuple{}
		for i := 0; i < len(partitions); i++ {
			// max := 0.0
			// maxj := 0
			for j := i + 1; j < len(partitions); j++ {
				// Number of common vertices between the two
				pullMatrix = append(pullMatrix, Tuple{
					partition1: i,
					partition2: j,
					similarity: sigs[i].Similarity(sigs[j]),
				})
				// pullMatrix[i] = append(pullMatrix[i],
				// 	partitions[i].Vertices.Intersect(partitions[j].Vertices).Cardinality())
				// if pullMatrix[i][j] > max {
				// 	max = pullMatrix[i][j]
				// 	maxj = j
				// }

			}
			// merge[maxj] = append(merge[maxj], i)
		}
		sort.Slice(pullMatrix, func(i, j int) bool {
			return pullMatrix[i].similarity > pullMatrix[j].similarity
		})
		fmt.Println(pullMatrix[0])
		if len(partitions) <= 10 {
			break
		}
		fmt.Println("Merging the partitions")
		partitions = mergePartitions(pullMatrix[0], partitions)
		fmt.Println(len(partitions))
		sigs[pullMatrix[0].partition1].Merge(sigs[pullMatrix[0].partition2])
		sigs = append(sigs[:pullMatrix[0].partition2], sigs[pullMatrix[0].partition2+1:]...)
	}

	// for k := 0; k < cap(partitions); k++ {
	// 	_, ok := merge[k]
	// 	if ok {
	// 		for j := range merge[k] {
	// 			partitions[k].Vertices = partitions[k].Vertices.Union(partitions[j].Vertices)
	// 			partitions[k].Edges = append(partitions[k].Edges, partitions[j].Edges...)
	// 			partitions[j].Vertices = mapset.NewSet()
	// 			partitions[j].Edges = []Edge{}
	// 		}
	//
	// 	}
	//
	// 	// partitions[k].vertices = partitions[k].vertices.Union
	// }

	return partitions
}

func leastLoaded(partitions mapset.Set) int {
	// If the partiton set is empty it means that we just consider all partitions
	// fmt.Println(partitions)
	if partitions == nil {
		// fmt.Println("Partitions are empty. Filling them up")
		partitions = mapset.NewSet()
		for k, _ := range partitionSizes {
			// fmt.Println(k)
			partitions.Add(k)
		}
	}
	// fmt.Println(partitions)
	// Get the leastloaded shit
	min := math.MaxInt32
	minp := math.MaxInt32
	for p := range partitions.Iterator().C {
		// fmt.Println(p.(int))
		// fmt.Println(partitionSizes[p.(int)])
		if partitionSizes[p.(int)] < min {
			min = partitionSizes[p.(int)]
			minp = p.(int)
		}
	}
	return minp
}

func init() {
	pDegree = make(map[int]int)
	partitionSizes = make(map[int]int)
	// for i := 0; i < totalpartitions; i += 1 {
	// 	partitionSizes[i] = 0
	// }
	membership = make(map[int]mapset.Set)
}