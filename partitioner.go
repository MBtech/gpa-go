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

func Partitioner(filename, sep string, numPartitions int, algo string) []Partition {

	file, err := os.Open(filename)
	// file, err := os.Open("testgraph.txt")
	if err != nil {
		panic("couldn't read the file")
	}

	defer file.Close()

	edges := []Edge{}
	scanner := bufio.NewScanner(file)
	totalPart := numPartitions
	partitions := []Partition{}
	if algo == "eti" {
		partitions = make([]Partition, 100)
	} else {
		partitions = make([]Partition, totalPart)
	}

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
			p := 0
			if algo == "hdrf" {
				p = HDRFPartitioner(source, destination, totalPart, partitions)
			} else if algo == "greedy" {
				p = GreedyPartitioner(source, destination, totalPart)
			} else if algo == "dbh" {
				p = DBHPartitioner(source, destination, totalPart)
			} else if algo == "hash" {
				p = HashPartitioner(source, destination, totalPart)
			} else {
				// In case of ETI partition
				p = HashPartitioner(source, destination, 100)
			}

			// fmt.Println(p)
			partitions[p].Vertices.Add(source)
			partitions[p].Vertices.Add(destination)
			partitions[p].Edges = append(partitions[p].Edges, e)
			edges = append(edges, e)
		}
	}
	if algo == "eti" {
		partitions = ETIPartitioner(partitions, totalPart)
	}

	fmt.Printf("Partitioning time %d s\n", time.Since(start)/1000000000)
	return partitions
	// fmt.Println(partitions[0].vertices)
}
