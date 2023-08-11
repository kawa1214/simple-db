package plan

import (
	"github.com/kawa1214/simple-db/pkg/db/record"
)

type Plan interface {

	// Open opens a scan corresponding to this plan.
	// The scan will be positioned before its first record.
	// Open() query.Scan

	// BlocksAccessed returns an estimate of the number of block accesses
	// that will occur when the scan is read to completion.
	BlocksAccessed() int

	// RecordsOutput returns an estimate of the number of records
	// in the query's output table.
	RecordsOutput() int

	// DistinctValues returns an estimate of the number of distinct values
	// for the specified field in the query's output table.
	DistinctValues(fldname string) int

	// Schema returns the schema of the query.
	Schema() record.Schema
}
