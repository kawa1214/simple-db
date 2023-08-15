package materialize

import (
	"github.com/kawa1214/simple-db/pkg/db/query"
	"github.com/kawa1214/simple-db/pkg/db/record"
)

// GroupByScan implements the scan for the groupby operator.
type GroupByScan struct {
	s           query.Scan
	groupfields []string
	aggfns      []AggregationFn
	groupval    *GroupValue
	moregroups  bool
}

// NewGroupByScan creates a groupby scan, given a grouped table scan.
func NewGroupByScan(s query.Scan, groupfields []string, aggfns []AggregationFn) *GroupByScan {
	return &GroupByScan{
		s:           s,
		groupfields: groupfields,
		aggfns:      aggfns,
	}
}

// BeforeFirst positions the scan before the first group.
func (g *GroupByScan) BeforeFirst() {
	g.s.BeforeFirst()
	g.moregroups = g.s.Next()
}

// Next moves to the next group.
func (g *GroupByScan) Next() bool {
	if !g.moregroups {
		return false
	}
	for _, fn := range g.aggfns {
		fn.ProcessFirst(g.s)
	}
	g.groupval = NewGroupValue(g.s, g.groupfields)
	for g.moregroups = g.s.Next(); g.moregroups; g.moregroups = g.s.Next() {
		gv := NewGroupValue(g.s, g.groupfields)
		if !g.groupval.Equals(gv) {
			break
		}
		for _, fn := range g.aggfns {
			fn.ProcessNext(g.s)
		}
	}
	return true
}

// Close closes the underlying scan.
func (g *GroupByScan) Close() {
	g.s.Close()
}

// GetVal gets the constant value of the specified field.
func (g *GroupByScan) GetVal(fldname string) *record.Constant {
	for _, fld := range g.groupfields {
		if fld == fldname {
			return g.groupval.GetVal(fldname)
		}
	}
	for _, fn := range g.aggfns {
		if fn.FieldName() == fldname {
			return fn.Value()
		}
	}
	return nil
}

// GetInt gets the integer value of the specified field.
func (g *GroupByScan) GetInt(fldname string) int {
	val := g.GetVal(fldname)

	return val.AsInt()
}

// GetString gets the string value of the specified field.
func (g *GroupByScan) GetString(fldname string) string {
	val := g.GetVal(fldname)
	return val.AsString()
}

// HasField returns true if the specified field is either a grouping field or created by an aggregation function.
func (g *GroupByScan) HasField(fldname string) bool {
	for _, fld := range g.groupfields {
		if fld == fldname {
			return true
		}
	}
	for _, fn := range g.aggfns {
		if fn.FieldName() == fldname {
			return true
		}
	}
	return false
}
