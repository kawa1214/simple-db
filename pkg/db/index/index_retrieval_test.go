package index

import (
	"fmt"
	"testing"

	"github.com/kawa1214/simple-db/pkg/db/buffer"
	"github.com/kawa1214/simple-db/pkg/db/file"
	"github.com/kawa1214/simple-db/pkg/db/log"
	"github.com/kawa1214/simple-db/pkg/db/metadata"
	"github.com/kawa1214/simple-db/pkg/db/plan"
	"github.com/kawa1214/simple-db/pkg/db/query"
	"github.com/kawa1214/simple-db/pkg/db/record"
	"github.com/kawa1214/simple-db/pkg/db/tx"
	"github.com/kawa1214/simple-db/pkg/util"
)

func TestIndexRetrieval(t *testing.T) {
	rootDir := util.ProjectRootDir()
	dir := rootDir + "/.tmp/studentdb"
	fm := file.NewFileMgr(dir, 400)
	lm := log.NewLogMgr(fm, "testlogfile")
	bm := buffer.NewBufferMgr(fm, lm, 8)

	tx := tx.NewTransaction(fm, lm, bm)
	mdm := metadata.NewMetadataMgr(true, tx)

	// Open a scan on the data table.
	studentPlan := plan.NewTablePlan(tx, "student", mdm)
	studentScan := studentPlan.Open().(query.UpdateScan)

	// Open the index on MajorId.
	indexes := mdm.GetIndexInfo("student", tx)
	ii := indexes["majorid"]
	idx := Open(ii)

	// Retrieve all index records having a dataval of 20.
	idx.BeforeFirst(record.NewIntConstant(20))
	for idx.Next() {
		// Use the datarid to go to the corresponding STUDENT record.
		dataRid := idx.GetDataRid()
		studentScan.MoveToRid(dataRid)
		fmt.Println(studentScan.GetString("sname"))
	}

	// Close the index and the data table.
	idx.Close()
	studentScan.Close()
	tx.Commit()
}
