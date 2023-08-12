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

func TestPlanner1(t *testing.T) {
	rootDir := util.ProjectRootDir()
	dir := rootDir + "/.tmp/plannertest1"
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
	fmt.Println("Inserting", n, "random records.")
	for i := 0; i < n; i++ {
		a := int(math.Round(rand.Float64() * 50))
		b := "rec" + fmt.Sprint(a)
		cmd = fmt.Sprintf("insert into T1(A,B) values(%d, '%s')", a, b)
		_ = planner.ExecuteUpdate(cmd, tx)
	}

	qry := "select B from T1 where A=10"
	p, err := planner.CreateQueryPlan(qry, tx)
	if err != nil {
		panic(err)
	}
	s := p.Open()

	for s.Next() {
		fmt.Println(s.GetString("b"))
	}
	s.Close()
	tx.Commit()
}
