package partitioning

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/deckarep/golang-set"
)

func Partitioner(filename, sep string, numPartitions int) []Partition {

	file, err := os.Open(filename)
	// file, err := os.Open("testgraph.txt")
	if err != nil {
		panic("couldn't read the file")
	}

	defer file.Close()

	edges := []Edge{}
	scanner := bufio.NewScanner(file)
	totalPart := numPartitions
	partitions := make([]Partition, totalPart)

	for i, _ := range partitions {
		partitions[i].Vertices = mapset.NewSet()
	}
	start := time.Now()
	for scanner.Scan() {
		s := scanner.Text()
		if !strings.Contains(s, "#") {

			split := strings.Split(s, sep)
			source, _ := strconv.Atoi(split[0])
			destination, _ := strconv.Atoi(split[1])
			e := Edge{
				Src:   source,
				Dest:  destination,
				Value: 0,
			}
			// p := HashPartitioner(source, destination, totalPart)
			// p := DBHPartitioner(source, destination, totalPart)
			// p := GreedyPartitioner(source, destination, totalPart, partitions)
			p := HDRFPartitioner(source, destination, totalPart, partitions)
			// fmt.Println(p)
			partitions[p].Vertices.Add(source)
			partitions[p].Vertices.Add(destination)
			partitions[p].Edges = append(partitions[p].Edges, e)
			edges = append(edges, e)
		}
	}
	// partitions = ETIPartitioner(partitions)
	totalV := 0
	for _, part := range partitions {
		fmt.Println(part.Vertices.Cardinality())
		fmt.Println(len(part.Edges))
		totalV += part.Vertices.Cardinality()
	}

	fmt.Println(totalV)
	fmt.Printf("Partitioning time %d s\n", time.Since(start)/1000000000)
	return partitions
	// fmt.Println(partitions[0].vertices)
}
