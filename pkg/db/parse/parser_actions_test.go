package parse

import (
	"fmt"
	"strings"
	"testing"
)

func TestParserActions(t *testing.T) {
	s := "select * from Sailors"
	p := NewParser(s)
	var result string
	if strings.HasPrefix(s, "select") {
		q := p.Query()
		result = q.String()
	} else {
		cmd := p.UpdateCmd()
		result = fmt.Sprintf("%T", cmd)
	}

	if result != "" {
		t.Logf("Your statement is: %v", result)
	} else {
		t.Logf("Your statement is illegal")
	}

	// t.Error()
}
