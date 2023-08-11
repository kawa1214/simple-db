package query

import "github.com/kawa1214/simple-db/pkg/db/record"

type Scan interface {
	// BeforeFirst positions the scan before its first record.
	// A subsequent call to Next will return the first record.
	BeforeFirst()

	// Next moves the scan to the next record.
	// Returns false if there is no next record.
	Next() bool

	// GetInt returns the value of the specified integer field in the current record.
	// The fldname parameter represents the name of the field.
	GetInt(fldname string) int

	// GetString returns the value of the specified string field in the current record.
	// The fldname parameter represents the name of the field.
	GetString(fldname string) string

	// GetVal returns the value of the specified field in the current record, expressed as a Constant.
	// The fldname parameter represents the name of the field.
	GetVal(fldname string) *record.Constant

	// HasField checks if the scan has the specified field.
	// The fldname parameter represents the name of the field.
	// Returns true if the scan has that field.
	HasField(fldname string) bool

	// Close closes the scan and its subscans, if any.
	Close()
}
