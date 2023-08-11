package parse

import (
	"bufio"
	"strconv"
	"strings"
)

type TokenType int

const (
	EOF TokenType = iota
	Word
	Number
	Other
)

// Lexer is the lexical analyzer.
type Lexer struct {
	keywords map[string]bool
	tok      *bufio.Scanner
	ttype    rune
	sval     string
	nval     int
}

// NewLexer creates a new lexical analyzer for SQL statement s.
func NewLexer(s string) *Lexer {
	l := &Lexer{
		keywords: initKeywords(),
		tok:      bufio.NewScanner(strings.NewReader(s)),
	}
	l.tok.Split(bufio.ScanWords)
	l.nextToken()
	return l
}

// matchDelim returns true if the current token is the specified delimiter character.
func (l *Lexer) MatchDelim(d rune) bool {
	// ttype == 'W and sval == d
	// '=' のケースに対応
	// if l.MatchKeyword(string(d)) && len(l.sval) == 1 {
	// 	return rune(l.sval[0]) == d
	// }
	return d == rune(l.sval[0])
}

// matchIntConstant returns true if the current token is an integer.
func (l *Lexer) matchIntConstant() bool {
	return l.ttype == 'N' // Assuming 'N' represents a number
}

// matchStringConstant returns true if the current token is a string.
func (l *Lexer) MatchStringConstant() bool {
	// return l.ttype == 'S' // Assuming 'S' represents a string
	return rune(l.sval[0]) == '\''
}

// matchKeyword returns true if the current token is the specified keyword.
func (l *Lexer) MatchKeyword(w string) bool {
	return l.ttype == 'W' && l.sval == w // Assuming 'W' represents a word
}

// matchId returns true if the current token is a legal identifier.
func (l *Lexer) MatchId() bool {
	return l.ttype == 'W' && !l.keywords[l.sval]
}

// eatDelim throws an exception if the current token is not the specified delimiter. Otherwise, moves to the next token.
func (l *Lexer) EatDelim(d rune) {
	if !l.MatchDelim(d) {
		panic("BadSyntaxException")
	}
	l.nextToken()
}

// eatIntConstant throws an exception if the current token is not an integer. Otherwise, returns that integer and moves to the next token.
func (l *Lexer) EatIntConstant() int {
	if !l.matchIntConstant() {
		panic("BadSyntaxException")
	}
	i := l.nval
	l.nextToken()
	return i
}

// eatStringConstant throws an exception if the current token is not a string. Otherwise, returns that string and moves to the next token.
func (l *Lexer) EatStringConstant() string {
	if !l.MatchStringConstant() {
		panic("BadSyntaxException")
	}
	s := l.sval
	l.nextToken()
	return s
}

// eatKeyword throws an exception if the current token is not the specified keyword. Otherwise, moves to the next token.
func (l *Lexer) EatKeyword(w string) {
	if !l.MatchKeyword(w) {
		panic("BadSyntaxException")
	}
	l.nextToken()
}

// eatId throws an exception if the current token is not an identifier. Otherwise, returns the identifier string and moves to the next token.
func (l *Lexer) EatId() string {
	if !l.MatchId() {
		panic("BadSyntaxException")
	}
	s := l.sval
	l.nextToken()
	return s
}

func (l *Lexer) nextToken() {
	if l.tok.Scan() {
		// Here, we're making a simple assumption about token types. You might need to adjust this based on your actual needs.
		token := l.tok.Text()
		if _, err := strconv.Atoi(token); err == nil {
			l.ttype = 'N'
			l.nval, _ = strconv.Atoi(token)
		} else if strings.HasPrefix(token, "'") && strings.HasSuffix(token, "'") {
			l.ttype = 'S'
			l.sval = token[1 : len(token)-1]
		} else {
			l.ttype = 'W'
			l.sval = strings.ToLower(token)
		}
	} else {
		l.ttype = -1
		l.ttype = '.'
	}
}

func initKeywords() map[string]bool {
	return map[string]bool{
		"select": true, "from": true, "where": true, "and": true,
		"insert": true, "into": true, "values": true, "delete": true, "update": true, "set": true,
		"create": true, "table": true, "int": true, "varchar": true, "view": true, "as": true, "index": true, "on": true,
	}
}
