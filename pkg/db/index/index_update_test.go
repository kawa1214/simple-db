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
	"github.com/kawa1214/simple-db/pkg/db/tx"
	"github.com/kawa1214/simple-db/pkg/util"
)

func TestIndexUpdate(t *testing.T) {
	rootDir := util.ProjectRootDir()
	dir := rootDir + "/.tmp/studentdb"
	fm := file.NewFileMgr(dir, 400)
	lm := log.NewLogMgr(fm, "testlogfile")
	bm := buffer.NewBufferMgr(fm, lm, 8)

	tx := tx.NewTransaction(fm, lm, bm)
	mdm := metadata.NewMetadataMgr(false, tx)
	studentPlan := plan.NewTablePlan(tx, "student", mdm)
	studentScan := studentPlan.Open().(query.UpdateScan)

	// Create a map containing all indexes for STUDENT.
	indexes := make(map[string]Index)
	idxInfo := mdm.GetIndexInfo("student", tx)
	for fldName, ii := range idxInfo {
		idx := Open(ii)
		indexes[fldName] = idx
	}

	// Task 1: insert a new STUDENT record for Sam
	//    First, insert the record into STUDENT.
	studentScan.Insert()
	studentScan.SetInt("studentid", 11)
	studentScan.SetString("sname", "sam")
	studentScan.SetInt("gradyear", 2023)
	studentScan.SetInt("majorid", 30)

	//    Then insert a record into each of the indexes.
	dataRid := studentScan.GetRid()
	for fldName, idx := range indexes {
		dataVal := studentScan.GetVal(fldName)
		idx.Insert(dataVal, dataRid)
	}

	// Task 2: find and delete Joe's record
	studentScan.BeforeFirst()
	for studentScan.Next() {
		if studentScan.GetString("sname") == "joe" {

			// First, delete the index records for Joe.
			joeRid := studentScan.GetRid()
			for fldName, idx := range indexes {
				dataVal := studentScan.GetVal(fldName)
				idx.Delete(dataVal, joeRid)
			}

			// Then delete Joe's record in STUDENT.
			studentScan.Delete()
			break
		}
	}

	// Print the records to verify the updates.
	studentScan.BeforeFirst()
	for studentScan.Next() {
		fmt.Println(studentScan.GetString("sname"), studentScan.GetInt("sid"))
	}
	studentScan.Close()

	for _, idx := range indexes {
		idx.Close()
	}
	tx.Commit()
}
