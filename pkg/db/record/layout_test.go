package record

import (
	"testing"
)

func TestLayout(t *testing.T) {
	sch := NewSchema()
	sch.AddIntField("A")
	sch.AddStringField("B", 9)
	layout := NewLayoutFromSchema(sch)

	for _, fldname := range layout.Schema().Fields {
		offset := layout.Offset(fldname)
		t.Logf("%s has offset %d\n", fldname, offset)
	}

	t.Error()
}
