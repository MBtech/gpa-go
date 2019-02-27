package tests

import (
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
