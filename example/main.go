package main

import (
	"database/sql"
	"fmt"
	"log"
	"math"
	"math/rand"

	_ "github.com/kawa1214/simple-db/pkg/db/driver"
	"github.com/kawa1214/simple-db/pkg/util"
)

func main() {
	name := util.RandomString(30)
	db, err := sql.Open("simple", name)
	if err != nil {
		log.Panic(err)
	}
	defer db.Close()

	query := "create table T1(A int, B varchar(9))"
	db.Exec(query)

	n := 200
	log.Print("Inserting", n, "random records.")
	for i := 0; i < n; i++ {
		a := int(math.Round(rand.Float64() * 50))
		b := "rec" + fmt.Sprint(a)
		db.Exec(fmt.Sprintf("insert into T1(A,B) values(%d, '%s')", a, b))
	}

	query = "select B from T1 where A=10"
	rows, err := db.Query(query)
	if err != nil {
		log.Panic(err)
	}
	defer rows.Close()

	fields, err := rows.Columns()
	if err != nil {
		log.Panic(err)
	}
	log.Print(fields)
	for rows.Next() {
		var b string
		rows.Scan(&b)
		log.Print(b)
	}
}
