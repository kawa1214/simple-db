package record

import "github.com/kawa1214/simple-db/pkg/db/file"

// INTEGER constant to represent Integer type
const INTEGER = 0 // You might want to define this elsewhere or use an existing constant

type Layout struct {
	schema   *Schema
	offsets  map[string]int
	slotsize int
}

func NewLayoutFromSchema(schema *Schema) *Layout {
	l := &Layout{
		schema:  schema,
		offsets: make(map[string]int),
	}
	pos := 4 // equivalent to Integer.BYTES in Java; 4 bytes for an int32
	for _, fldname := range schema.Fields {
		l.offsets[fldname] = pos
		pos += l.lengthInBytes(fldname)
	}
	l.slotsize = pos
	return l
}

func NewLayout(schema *Schema, offsets map[string]int, slotsize int) *Layout {
	return &Layout{
		schema:   schema,
		offsets:  offsets,
		slotsize: slotsize,
	}
}

func (l *Layout) Schema() *Schema {
	return l.schema
}

func (l *Layout) Offset(fldname string) int {
	return l.offsets[fldname]
}

func (l *Layout) SlotSize() int {
	return l.slotsize
}

func (l *Layout) lengthInBytes(fldname string) int {
	fldtype, err := l.schema.Type(fldname)
	if err != nil {
		panic(err)
	}
	if fldtype == INTEGER {
		return 4 // equivalent to Integer.BYTES in Java; 4 bytes for an int32
	}
	// else clause for VARCHAR type
	len, err := l.schema.Length(fldname)
	if err != nil {
		panic(err)
	}
	return file.MaxLength(len)
}
