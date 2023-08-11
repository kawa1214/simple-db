package parse

import (
	"github.com/kawa1214/simple-db/pkg/db/query"
	"github.com/kawa1214/simple-db/pkg/db/record"
)

// Parser is the SimpleDB parser.
type Parser struct {
	lex *Lexer
}

// NewParser creates a new parser for SQL statement s.
func NewParser(s string) *Parser {
	return &Parser{lex: NewLexer(s)}
}

// Field parses and returns a field.
func (p *Parser) Field() string {
	return p.lex.EatId()
}

// Constant parses and returns a constant.
func (p *Parser) Constant() *record.Constant {
	if p.lex.MatchStringConstant() {
		return record.NewStringConstant(p.lex.EatStringConstant())
	}
	return record.NewIntConstant(p.lex.EatIntConstant())
}

// Expression parses and returns an expression.
func (p *Parser) Expression() *query.Expression {
	if p.lex.MatchId() {
		return query.NewFieldExpression(p.Field())
	}
	return query.NewConstantExpression(p.Constant())
}

// Term parses and returns a term.
func (p *Parser) Term() *query.Term {
	lhs := p.Expression()
	p.lex.EatDelim('=')
	rhs := p.Expression()
	return query.NewTerm(*lhs, *rhs)
}

// Predicate parses and returns a predicate.
func (p *Parser) Predicate() *query.Predicate {
	pred := query.NewPredicate(p.Term())
	if p.lex.MatchKeyword("and") {
		p.lex.EatKeyword("and")
		pred.ConjoinWith(p.Predicate())
	}
	return pred
}

// Query parses and returns a query data.
func (p *Parser) Query() *QueryData {
	p.lex.EatKeyword("select")
	fields := p.selectList()
	p.lex.EatKeyword("from")
	tables := p.tableList()
	pred := &query.Predicate{}
	if p.lex.MatchKeyword("where") {
		p.lex.EatKeyword("where")
		pred = p.Predicate()
	}
	return NewQueryData(fields, tables, pred)
}

func (p *Parser) selectList() []string {
	L := []string{p.Field()}
	if p.lex.MatchDelim(',') {
		p.lex.EatDelim(',')
		L = append(L, p.selectList()...)
	}
	return L
}

func (p *Parser) tableList() []string {
	L := []string{p.lex.EatId()}
	if p.lex.MatchDelim(',') {
		p.lex.EatDelim(',')
		L = append(L, p.tableList()...)
	}
	return L
}

// UpdateCmd parses and returns an update command.
func (p *Parser) UpdateCmd() interface{} {
	if p.lex.MatchKeyword("insert") {
		return p.Insert()
	} else if p.lex.MatchKeyword("delete") {
		return p.Delete()
	} else if p.lex.MatchKeyword("update") {
		return p.Modify()
	}
	return p.create()
}

func (p *Parser) create() interface{} {
	p.lex.EatKeyword("create")
	if p.lex.MatchKeyword("table") {
		return p.CreateTable()
	} else if p.lex.MatchKeyword("view") {
		return p.CreateView()
	}
	return p.CreateIndex()
}

// Delete parses and returns a delete data.
func (p *Parser) Delete() *DeleteData {
	p.lex.EatKeyword("delete")
	p.lex.EatKeyword("from")
	tblname := p.lex.EatId()
	pred := &query.Predicate{}
	if p.lex.MatchKeyword("where") {
		p.lex.EatKeyword("where")
		pred = p.Predicate()
	}
	return NewDeleteData(tblname, pred)
}

// Insert parses and returns an insert data.
func (p *Parser) Insert() *InsertData {
	p.lex.EatKeyword("insert")
	p.lex.EatKeyword("into")
	tblname := p.lex.EatId()
	p.lex.EatDelim('(')
	flds := p.fieldList()
	p.lex.EatDelim(')')
	p.lex.EatKeyword("values")
	p.lex.EatDelim('(')
	vals := p.constList()
	p.lex.EatDelim(')')
	return NewInsertData(tblname, flds, vals)
}

func (p *Parser) fieldList() []string {
	L := []string{p.Field()}
	if p.lex.MatchDelim(',') {
		p.lex.EatDelim(',')
		L = append(L, p.fieldList()...)
	}
	return L
}

func (p *Parser) constList() []*record.Constant {
	L := []*record.Constant{p.Constant()}
	if p.lex.MatchDelim(',') {
		p.lex.EatDelim(',')
		L = append(L, p.constList()...)
	}
	return L
}

// Modify parses and returns a modify data.
func (p *Parser) Modify() *ModifyData {
	p.lex.EatKeyword("update")
	tblname := p.lex.EatId()
	p.lex.EatKeyword("set")
	fldname := p.Field()
	p.lex.EatDelim('=')
	newval := p.Expression()
	pred := &query.Predicate{}
	if p.lex.MatchKeyword("where") {
		p.lex.EatKeyword("where")
		pred = p.Predicate()
	}
	return NewModifyData(tblname, fldname, newval, pred)
}

// CreateTable parses and returns a create table data.
func (p *Parser) CreateTable() *CreateTableData {
	p.lex.EatKeyword("table")
	tblname := p.lex.EatId()
	p.lex.EatDelim('(')
	sch := p.fieldDefs()
	p.lex.EatDelim(')')
	return NewCreateTableData(tblname, sch)
}

func (p *Parser) fieldDefs() *record.Schema {
	schema := p.fieldDef()
	if p.lex.MatchDelim(',') {
		p.lex.EatDelim(',')
		schema2 := p.fieldDefs()
		schema.AddAll(schema2)
	}
	return schema
}

func (p *Parser) fieldDef() *record.Schema {
	fldname := p.Field()
	return p.fieldType(fldname)
}

func (p *Parser) fieldType(fldname string) *record.Schema {
	schema := record.NewSchema()
	if p.lex.MatchKeyword("int") {
		p.lex.EatKeyword("int")
		schema.AddIntField(fldname)
	} else {
		p.lex.EatKeyword("varchar")
		p.lex.EatDelim('(')
		strLen := p.lex.EatIntConstant()
		p.lex.EatDelim(')')
		schema.AddStringField(fldname, strLen)
	}
	return schema
}

// CreateView parses and returns a create view data.
func (p *Parser) CreateView() *CreateViewData {
	p.lex.EatKeyword("view")
	viewname := p.lex.EatId()
	p.lex.EatKeyword("as")
	qd := p.Query()
	return NewCreateViewData(viewname, qd)
}

// CreateIndex parses and returns a create index data.
func (p *Parser) CreateIndex() *CreateIndexData {
	p.lex.EatKeyword("index")
	idxname := p.lex.EatId()
	p.lex.EatKeyword("on")
	tblname := p.lex.EatId()
	p.lex.EatDelim('(')
	fldname := p.Field()
	p.lex.EatDelim(')')
	return NewCreateIndexData(idxname, tblname, fldname)
}
