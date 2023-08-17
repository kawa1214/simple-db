package plan

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/kawa1214/simple-db/pkg/db/buffer"
	"github.com/kawa1214/simple-db/pkg/db/file"
	"github.com/kawa1214/simple-db/pkg/db/log"
	"github.com/kawa1214/simple-db/pkg/db/metadata"
	"github.com/kawa1214/simple-db/pkg/db/tx"
	"github.com/kawa1214/simple-db/pkg/util"
)

func TestPlannerStudent(t *testing.T) {
	rootDir := util.ProjectRootDir()
	dir := rootDir + "/.tmp/studentdb"
	fm := file.NewFileMgr(dir, 400)
	lm := log.NewLogMgr(fm, "testlogfile")
	bm := buffer.NewBufferMgr(fm, lm, 8)

	tx := tx.NewTransaction(fm, lm, bm)
	mdm := metadata.NewMetadataMgr(true, tx)
	qp := NewBasicQueryPlanner(mdm)
	up := NewBasicUpdatePlanner(mdm)
	planner := NewPlanner(qp, up)

	cmd := "create table student(sname varchar(10), gradyear int, majorid int, studentid int)"
	_ = planner.ExecuteUpdate(cmd, tx)
	tx.Commit()

	for i := 0; i < 100; i++ {
		a := "rec" + fmt.Sprint(i)
		b := int(math.Round(rand.Float64() * 50))
		cmd = fmt.Sprintf("insert into student(sname, gradyear, majorid, studentid) values('%s', %d, %d, %d)", a, b, i, i)
		_ = planner.ExecuteUpdate(cmd, tx)
	}

	// part 1: Process a query
	qry := "select sname, gradyear from student"
	p, err := planner.CreateQueryPlan(qry, tx)
	if err != nil {
		panic(err)
	}
	s := p.Open()

	for s.Next() {
		fmt.Println(s.GetString("sname"), s.GetInt("gradyear"))
	}
	s.Close()

	// part 2: Process an update command
	cmd = "delete from STUDENT where MajorId = 30"
	num := planner.ExecuteUpdate(cmd, tx)

	fmt.Println(num, "students were deleted")

	tx.Commit()
	// t.Error()
}
