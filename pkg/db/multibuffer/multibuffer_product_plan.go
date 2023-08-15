package multibuffer

import (
	"github.com/kawa1214/simple-db/pkg/db/materialize"
	"github.com/kawa1214/simple-db/pkg/db/plan"
	"github.com/kawa1214/simple-db/pkg/db/query"
	"github.com/kawa1214/simple-db/pkg/db/record"
	"github.com/kawa1214/simple-db/pkg/db/tx"
)

// MultibufferProductPlan represents the plan class for the multi-buffer version of the product operator.
type MultibufferProductPlan struct {
	tx     *tx.Transaction
	lhs    plan.Plan
	rhs    plan.Plan
	schema *record.Schema
}

// NewMultibufferProductPlan creates a product plan for the specified queries.
func NewMultibufferProductPlan(tx *tx.Transaction, lhs plan.Plan, rhs plan.Plan) *MultibufferProductPlan {
	mpp := &MultibufferProductPlan{
		tx:  tx,
		lhs: materialize.NewMaterializePlan(tx, lhs),
		rhs: rhs,
	}
	mpp.schema = record.NewSchema()
	mpp.schema.AddAll(lhs.Schema())
	mpp.schema.AddAll(rhs.Schema())
	return mpp
}

// Open creates and returns a scan for this query.
func (mpp *MultibufferProductPlan) Open() query.Scan {
	leftscan := mpp.lhs.Open()
	tt := mpp.copyRecordsFrom(mpp.rhs)
	return NewMultibufferProductScan(mpp.tx, leftscan, tt.TableName(), tt.GetLayout())
}

// BlocksAccessed returns an estimate of the number of block accesses required to execute the query.
func (mpp *MultibufferProductPlan) BlocksAccessed() int {
	avail := mpp.tx.AvailableBuffs()
	size := materialize.NewMaterializePlan(mpp.tx, mpp.rhs).BlocksAccessed()
	numchunks := size / avail
	return mpp.rhs.BlocksAccessed() + (mpp.lhs.BlocksAccessed() * numchunks)
}

// RecordsOutput estimates the number of output records in the product.
func (mpp *MultibufferProductPlan) RecordsOutput() int {
	return mpp.lhs.RecordsOutput() * mpp.rhs.RecordsOutput()
}

// DistinctValues estimates the distinct number of field values in the product.
func (mpp *MultibufferProductPlan) DistinctValues(fldname string) int {
	if mpp.lhs.Schema().HasField(fldname) {
		return mpp.lhs.DistinctValues(fldname)
	}
	return mpp.rhs.DistinctValues(fldname)
}

// Schema returns the schema of the product.
func (mpp *MultibufferProductPlan) Schema() *record.Schema {
	return mpp.schema
}

func (mpp *MultibufferProductPlan) copyRecordsFrom(p plan.Plan) *materialize.TempTable {
	src := p.Open()
	sch := p.Schema()
	t := materialize.NewTempTable(mpp.tx, sch)
	dest := t.Open().(query.UpdateScan)
	for src.Next() {
		dest.Insert()
		for _, fldname := range sch.Fields {
			dest.SetVal(fldname, src.GetVal(fldname))
		}
	}
	src.Close()
	dest.Close()
	return t
}
