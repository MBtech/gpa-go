package tests

import (
	"bufio"
	"hash/fnv"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/MBtech/gpa-go"
)

func TestHashPartitioner(t *testing.T) {
	source := 10
	destination := 2
	hsrc := 252472541 % 2
	hdest := 118251589 % 2
	pTest := (hsrc + hdest) % 2
	p := partitioning.HashPartitioner(source, destination, 2)
	if p != pTest {
		t.Error("Hash partitioner isn't working correctly")
	}
}

func TestDBHPartitioner(t *testing.T) {
	file, err := os.Open("../cmd/testgraph.txt")
	sep := " "
	if err != nil {
		panic("couldn't read the file")
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	totalPart := 3
	results := []int{}
	for _, v := range []int{2, 3, 4, 5, 6, 7, 12, 9, 10, 8, 8, 11} {
		h := fnv.New32a()
		h.Write([]byte(string(v)))
		results = append(results, int(h.Sum32())%totalPart)
	}

	i := 0
	for scanner.Scan() {
		s := scanner.Text()
		if !strings.Contains(s, "#") {

			split := strings.Split(s, sep)
			source, _ := strconv.Atoi(split[0])
			destination, _ := strconv.Atoi(split[1])
			p := partitioning.DBHPartitioner(source, destination, totalPart)
			if results[i] != p {
				t.Errorf("Mismatched partition decisions for src:%d dest:%d: partition %d should be %d", source, destination, p, results[i])
			}
			i++
		}
	}

}
