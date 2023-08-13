package driver

import (
	"database/sql"
	"database/sql/driver"

	"github.com/kawa1214/simple-db/pkg/db/buffer"
	"github.com/kawa1214/simple-db/pkg/db/file"
	"github.com/kawa1214/simple-db/pkg/db/log"
	"github.com/kawa1214/simple-db/pkg/db/metadata"
	"github.com/kawa1214/simple-db/pkg/db/plan"
	"github.com/kawa1214/simple-db/pkg/db/query"
	"github.com/kawa1214/simple-db/pkg/db/tx"
	"github.com/kawa1214/simple-db/pkg/util"
)

type SimpleDriver struct {
}

func NewSimpleDriver() *SimpleDriver {
	return &SimpleDriver{}
}

func (d *SimpleDriver) Open(name string) (driver.Conn, error) {
	return NewSimpleDBConn(name), nil
}

type SimpleConn struct {
	fm      *file.FileMgr
	bm      *buffer.BufferMgr
	lm      *log.LogMgr
	tx      *tx.Transaction
	mdm     *metadata.MetadataMgr
	planner *plan.Planner
}

func NewSimpleDBConn(name string) *SimpleConn {
	rootDir := util.ProjectRootDir()
	dir := rootDir + "/.tmp/" + name
	fm := file.NewFileMgr(dir, 800)
	lm := log.NewLogMgr(fm, "testlogfile")
	bm := buffer.NewBufferMgr(fm, lm, 8)
	tx := tx.NewTransaction(fm, lm, bm)

	isNew := fm.IsNew()

	if !isNew {
		tx.Recover()
	}

	mdm := metadata.NewMetadataMgr(isNew, tx)
	qp := plan.NewBasicQueryPlanner(mdm)
	up := plan.NewBasicUpdatePlanner(mdm)
	planner := plan.NewPlanner(qp, up)
	tx.Commit()

	return &SimpleConn{
		fm:      fm,
		bm:      bm,
		lm:      lm,
		tx:      tx,
		mdm:     mdm,
		planner: planner,
	}
}

func (conn *SimpleConn) Begin() (driver.Tx, error) {
	panic("unimplemented")
}

// クローズは、現在準備されているステートメントとトランザクションを無効にし、停止する可能性がある。
// 準備されたステートメントとトランザクションを停止し、この接続は使用されなくなります。
func (conn *SimpleConn) Close() error {

	return nil
}

func (conn *SimpleConn) Prepare(query string) (driver.Stmt, error) {
	return NewSimpleStmt(query, conn), nil
}

type SimpleStmt struct {
	conn  *SimpleConn
	query string
}

func NewSimpleStmt(query string, conn *SimpleConn) *SimpleStmt {
	return &SimpleStmt{
		query: query,
		conn:  conn,
	}
}

// クローズはステートメントを閉じる。
//
// Go 1.1では、ステートメントがクエリによって使用されている場合、ステートメントはクローズされません。
// クエリによって使用されている場合は閉じません。
// ドライバは、Closeによって行われるすべてのネットワーク呼び出しが無限にブロックされないようにする必要があります（タイムアウトを適用するなど）。
func (stmt *SimpleStmt) Close() error {
	stmt.conn.tx.Commit()
	return nil
}

// NumInputはプレースホルダ・パラメータの数を返す。
// NumInput が >= 0 を返す場合、SQL パッケージは呼び出し元からの
// 引数数のサニティチェックを行い、呼び出し元にエラーを返します。
// ステートメントの Exec メソッドや Query メソッドが呼び出される前に、 呼び出し元からの引数数をチェックし、呼び出し元にエラーを返します。
// ドライバがプレースホルダの数を知らない場合、NumInput は -1 を返すこともあります。
// そのプレースホルダの数をドライバが知らない場合、 NumInput は -1 を返すこともあります。
// その場合、SQLパッケージは Exec や Query の引数の数をチェックしません。
func (stmt *SimpleStmt) NumInput() int {
	// unimplimented
	return -1
}

func (stmt *SimpleStmt) Exec(args []driver.Value) (driver.Result, error) {
	stmt.conn.planner.ExecuteUpdate(stmt.query, stmt.conn.tx)
	return nil, nil
}

func (stmt *SimpleStmt) Query(args []driver.Value) (driver.Rows, error) {
	p, err := stmt.conn.planner.CreateQueryPlan(stmt.query, stmt.conn.tx)
	if err != nil {
		return nil, err
	}
	s := p.Open()
	fields := p.Schema().Fields
	return NewSimpleRows(s, fields), nil
}

type SimpleRows struct {
	s      query.Scan
	fields []string
}

func NewSimpleRows(s query.Scan, fields []string) *SimpleRows {
	return &SimpleRows{
		s:      s,
		fields: fields,
	}
}

func (rows *SimpleRows) Columns() []string {
	return rows.fields
}

func (rows *SimpleRows) Close() error {
	rows.s.Close()
	return nil
}

func (rows *SimpleRows) Next(dest []driver.Value) error {
	if !rows.s.Next() {
		return driver.ErrSkip
	}
	for i, field := range rows.fields {
		val := rows.s.GetVal(field)
		dest[i] = val.AnyValue()
	}
	return nil
}

func init() {
	sql.Register("simple", NewSimpleDriver())
}
