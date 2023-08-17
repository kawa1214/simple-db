package parse

import (
	"testing"
)

func TestPrefParser(t *testing.T) {
	s := "a = 1 and b = 2"
	p := NewPredParser(s)
	p.Predicate()

	// t.Error()
}
