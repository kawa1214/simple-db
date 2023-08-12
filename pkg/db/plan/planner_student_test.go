package plan

// func TestPlannerStudent(t *testing.T) {
// 	db := simpledb.NewSimpleDB("studentdb")
// 	planner := db.Planner()
// 	tx := db.NewTx()

// 	// part 1: Process a query
// 	qry := "select sname, gradyear from student"
// 	p, err := planner.CreateQueryPlan(qry, tx)
// 	if err != nil {
// 		panic(err)
// 	}
// 	s, err := p.Open()
// 	if err != nil {
// 		panic(err)
// 	}
// 	for s.Next() {
// 		fmt.Println(s.GetString("sname"), s.GetInt("gradyear"))
// 	}
// 	s.Close()

// 	// part 2: Process an update command
// 	cmd := "delete from STUDENT where MajorId = 30"
// 	num, err := planner.ExecuteUpdate(cmd, tx)
// 	if err != nil {
// 		panic(err)
// 	}
// 	fmt.Println(num, "students were deleted")

// 	tx.Commit()
// }
