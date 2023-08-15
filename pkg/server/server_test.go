package server

import (
	"database/sql"
	"fmt"
	"math"
	"math/rand"
	"testing"

	_ "github.com/kawa1214/simple-db/pkg/db/driver"
)

// TODO: move to pkg/db/driver/driver_test.go
func TestServer(t *testing.T) {
	name := randomString(30)
	db, err := sql.Open("simple", name)
	if err != nil {
		t.Error(err)
	}
	query := "create table T1(A int, B varchar(9))"
	db.Exec(query)

	n := 200
	t.Log("Inserting", n, "random records.")
	for i := 0; i < n; i++ {
		a := int(math.Round(rand.Float64() * 50))
		b := "rec" + fmt.Sprint(a)
		db.Exec(fmt.Sprintf("insert into T1(A,B) values(%d, '%s')", a, b))
	}

	query = "select B from T1 where A=10"
	rows, err := db.Query(query)
	if err != nil {
		t.Error(err)
	}

	fields, err := rows.Columns()
	if err != nil {
		t.Error(err)
	}
	t.Log(fields)
	for rows.Next() {
		var b string
		rows.Scan(&b)
		t.Log(b)
	}
	rows.Close()

	db.Close()

	t.Error()
}

func randomString(n int) string {
	var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
