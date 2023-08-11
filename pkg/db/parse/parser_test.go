package parse

import (
	"strings"
	"testing"
)

func TestParser(t *testing.T) {
	s := "select * from Sailors"
	p := NewParser(s)
	if strings.HasPrefix(s, "select") {
		q := p.Query()
		t.Logf("Query: %s", q.String())
	} else {
		c := p.UpdateCmd()
		t.Logf("UpdateCmd: %v", c)
	}

	t.Error()
}
