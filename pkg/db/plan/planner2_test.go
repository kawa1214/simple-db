package plan

import (
	"fmt"
	"testing"

	"github.com/kawa1214/simple-db/pkg/db/buffer"
	"github.com/kawa1214/simple-db/pkg/db/file"
	"github.com/kawa1214/simple-db/pkg/db/log"
	"github.com/kawa1214/simple-db/pkg/db/metadata"
	"github.com/kawa1214/simple-db/pkg/db/tx"
	"github.com/kawa1214/simple-db/pkg/util"
)

func TestPlanner2(t *testing.T) {
	rootDir := util.ProjectRootDir()
	dir := rootDir + "/.tmp/plannertest2"
	fm := file.NewFileMgr(dir, 400)
	lm := log.NewLogMgr(fm, "testlogfile")
	bm := buffer.NewBufferMgr(fm, lm, 8)

	tx := tx.NewTransaction(fm, lm, bm)
	mdm := metadata.NewMetadataMgr(true, tx)
	qp := NewBasicQueryPlanner(mdm)
	up := NewBasicUpdatePlanner(mdm)
	planner := NewPlanner(qp, up)

	cmd := "create table T1(A int, B varchar(9))"
	_ = planner.ExecuteUpdate(cmd, tx)

	n := 200
	fmt.Println("Inserting", n, "records into T1.")
	for i := 0; i < n; i++ {
		a := i
		b := "bbb" + fmt.Sprint(a)
		cmd = fmt.Sprintf("insert into T1(A,B) values(%d, '%s')", a, b)
		_ = planner.ExecuteUpdate(cmd, tx)
	}

	cmd = "create table T2(C int, D varchar(9))"
	_ = planner.ExecuteUpdate(cmd, tx)

	fmt.Println("Inserting", n, "records into T2.")
	for i := 0; i < n; i++ {
		c := n - i - 1
		d := "ddd" + fmt.Sprint(c)
		cmd = fmt.Sprintf("insert into T2(C,D) values(%d, '%s')", c, d)
		_ = planner.ExecuteUpdate(cmd, tx)
	}

	qry := "select B,D from T1,T2 where A=C"
	p, err := planner.CreateQueryPlan(qry, tx)
	if err != nil {
		panic(err)
	}
	s := p.Open()

	for s.Next() {
		fmt.Println(s.GetString("b"), s.GetString("d"))
	}
	s.Close()
	tx.Commit()
}
