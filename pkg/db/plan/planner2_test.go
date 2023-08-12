package plan

// func TestPlanner2(t *testing.T) {
// 	db := simpledb.NewSimpleDB("plannertest2")
// 	tx := db.NewTx()
// 	planner := db.Planner()

// 	cmd := "create table T1(A int, B varchar(9))"
// 	_, err := planner.ExecuteUpdate(cmd, tx)
// 	if err != nil {
// 		panic(err)
// 	}

// 	n := 200
// 	fmt.Println("Inserting", n, "records into T1.")
// 	for i := 0; i < n; i++ {
// 		a := i
// 		b := "bbb" + fmt.Sprint(a)
// 		cmd = fmt.Sprintf("insert into T1(A,B) values(%d, '%s')", a, b)
// 		_, err := planner.ExecuteUpdate(cmd, tx)
// 		if err != nil {
// 			panic(err)
// 		}
// 	}

// 	cmd = "create table T2(C int, D varchar(9))"
// 	_, err = planner.ExecuteUpdate(cmd, tx)
// 	if err != nil {
// 		panic(err)
// 	}
// 	fmt.Println("Inserting", n, "records into T2.")
// 	for i := 0; i < n; i++ {
// 		c := n - i - 1
// 		d := "ddd" + fmt.Sprint(c)
// 		cmd = fmt.Sprintf("insert into T2(C,D) values(%d, '%s')", c, d)
// 		_, err := planner.ExecuteUpdate(cmd, tx)
// 		if err != nil {
// 			panic(err)
// 		}
// 	}

// 	qry := "select B,D from T1,T2 where A=C"
// 	p, err := planner.CreateQueryPlan(qry, tx)
// 	if err != nil {
// 		panic(err)
// 	}
// 	s, err := p.Open()
// 	if err != nil {
// 		panic(err)
// 	}
// 	for s.Next() {
// 		fmt.Println(s.GetString("b"), s.GetString("d"))
// 	}
// 	s.Close()
// 	tx.Commit()
// }
