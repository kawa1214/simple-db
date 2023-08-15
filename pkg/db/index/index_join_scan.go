package index

import (
	"github.com/kawa1214/simple-db/pkg/db/query"
	"github.com/kawa1214/simple-db/pkg/db/record"
)

type IndexJoinScan struct {
	lhs       query.Scan
	idx       Index
	joinfield string
	rhs       *record.TableScan
}

func NewIndexJoinScan(lhs query.Scan, idx Index, joinfield string, rhs *record.TableScan) *IndexJoinScan {
	ijScan := &IndexJoinScan{
		lhs:       lhs,
		idx:       idx,
		joinfield: joinfield,
		rhs:       rhs,
	}
	ijScan.BeforeFirst()
	return ijScan
}

func (ijs *IndexJoinScan) BeforeFirst() {
	ijs.lhs.BeforeFirst()
	ijs.lhs.Next()
	ijs.resetIndex()
}

func (ijs *IndexJoinScan) Next() bool {
	for {
		if ijs.idx.Next() {
			ijs.rhs.MoveToRid(ijs.idx.GetDataRid())
			return true
		}
		if !ijs.lhs.Next() {
			return false
		}
		ijs.resetIndex()
	}
}

func (ijs *IndexJoinScan) GetInt(fldname string) int {
	if ijs.rhs.HasField(fldname) {
		return ijs.rhs.GetInt(fldname)
	}
	return ijs.lhs.GetInt(fldname)
}

func (ijs *IndexJoinScan) GetVal(fldname string) *record.Constant {
	if ijs.rhs.HasField(fldname) {
		return ijs.rhs.GetVal(fldname)
	}
	return ijs.lhs.GetVal(fldname)
}

func (ijs *IndexJoinScan) GetString(fldname string) string {
	if ijs.rhs.HasField(fldname) {
		return ijs.rhs.GetString(fldname)
	}
	return ijs.lhs.GetString(fldname)
}

func (ijs *IndexJoinScan) HasField(fldname string) bool {
	return ijs.rhs.HasField(fldname) || ijs.lhs.HasField(fldname)
}

func (ijs *IndexJoinScan) Close() {
	ijs.lhs.Close()
	ijs.idx.Close()
	ijs.rhs.Close()
}

func (ijs *IndexJoinScan) resetIndex() {
	searchkey := ijs.lhs.GetVal(ijs.joinfield)
	ijs.idx.BeforeFirst(searchkey)
}
