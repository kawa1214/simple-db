package index

import (
	"fmt"
	"testing"

	"github.com/kawa1214/simple-db/pkg/db/buffer"
	"github.com/kawa1214/simple-db/pkg/db/file"
	"github.com/kawa1214/simple-db/pkg/db/log"
	"github.com/kawa1214/simple-db/pkg/db/metadata"
	"github.com/kawa1214/simple-db/pkg/db/plan"
	"github.com/kawa1214/simple-db/pkg/db/record"
	"github.com/kawa1214/simple-db/pkg/db/tx"
	"github.com/kawa1214/simple-db/pkg/util"
)

func TestIndexSelect(t *testing.T) {
	rootDir := util.ProjectRootDir()
	dir := rootDir + "/.tmp/studentdb"
	fm := file.NewFileMgr(dir, 400)
	lm := log.NewLogMgr(fm, "testlogfile")
	bm := buffer.NewBufferMgr(fm, lm, 8)

	tx := tx.NewTransaction(fm, lm, bm)
	mdm := metadata.NewMetadataMgr(true, tx)

	// Find the index on StudentId.
	indexes := mdm.GetIndexInfo("enroll", tx)
	sidIdx := indexes["studentid"]

	// Get the plan for the Enroll table
	enrollPlan := plan.NewTablePlan(tx, "enroll", mdm)

	// Create the selection constant
	c := record.NewIntConstant(6)

	// Two different ways to use the index in simpledb:
	useIndexManually(sidIdx, enrollPlan, c)
	useIndexScan(sidIdx, enrollPlan, c)

	tx.Commit()
}

func useIndexManually(ii *metadata.IndexInfo, p plan.Plan, c *record.Constant) {
	// Open a scan on the table.
	s := p.Open().(*record.TableScan) // must be a table scan
	idx := Open(ii)

	// Retrieve all index records having the specified dataval.
	idx.BeforeFirst(c)
	for idx.Next() {
		// Use the datarid to go to the corresponding Enroll record.
		datarid := idx.GetDataRid()
		s.MoveToRid(datarid) // table scans can move to a specified RID.
		fmt.Println(s.GetString("grade"))
	}
	idx.Close()
	s.Close()
}

func useIndexScan(ii *metadata.IndexInfo, p plan.Plan, c *record.Constant) {
	// Open an index select scan on the enroll table.
	idxPlan := NewIndexSelectPlan(p, ii, c)
	s := idxPlan.Open()

	for s.Next() {
		fmt.Println(s.GetString("grade"))
	}
	s.Close()
}
