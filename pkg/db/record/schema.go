package record

import "errors"

type FieldInfo struct {
	Type   int
	Length int
}

// Schema represents the record schema of a table
type Schema struct {
	Fields []string
	Info   map[string]FieldInfo
}

// NewSchema creates and returns a new schema
func NewSchema() *Schema {
	return &Schema{
		Fields: make([]string, 0),
		Info:   make(map[string]FieldInfo),
	}
}

// AddField adds a field to the schema with the given name, type, and length
func (s *Schema) AddField(fldname string, fieldType int, length int) {
	s.Fields = append(s.Fields, fldname)
	s.Info[fldname] = FieldInfo{Type: fieldType, Length: length}
}

// AddIntField adds an integer field to the schema
func (s *Schema) AddIntField(fldname string) {
	s.AddField(fldname, Integer, 0)
}

// AddStringField adds a string field to the schema with the specified length
func (s *Schema) AddStringField(fldname string, length int) {
	s.AddField(fldname, Varchar, length)
}

// Add adds a field from another schema to this schema
func (s *Schema) Add(fldname string, sch *Schema) {
	fieldType, _ := sch.Type(fldname)
	length, _ := sch.Length(fldname)
	s.AddField(fldname, fieldType, length)
}

// AddAll adds all fields from another schema to this schema
func (s *Schema) AddAll(sch *Schema) {
	for _, fldname := range sch.Fields {
		s.Add(fldname, sch)
	}
}

// HasField checks if a field exists in the schema
func (s *Schema) HasField(fldname string) bool {
	for _, fld := range s.Fields {
		if fld == fldname {
			return true
		}
	}
	return false
}

// Type returns the type of the field
func (s *Schema) Type(fldname string) (int, error) {
	info, ok := s.Info[fldname]
	if !ok {
		return 0, errors.New("field not found")
	}
	return info.Type, nil
}

// Length returns the length of the field
func (s *Schema) Length(fldname string) (int, error) {
	info, ok := s.Info[fldname]
	if !ok {
		return 0, errors.New("field not found")
	}
	return info.Length, nil
}

// Constants representing SQL types
const (
	Integer = 1 // Placeholder, adjust as per your actual constants
	Varchar = 2 // Placeholder, adjust as per your actual constants
)
