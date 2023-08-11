package parse

import (
	"testing"
)

func TestLexer(t *testing.T) {
	text := "c = 1"
	// text := "1 = c"
	var x string
	var y int

	lex := NewLexer(text)
	if lex.MatchId() {
		x = lex.EatId()
		lex.EatDelim('=')
		y = lex.EatIntConstant()
	} else {
		y = lex.EatIntConstant()
		lex.EatDelim('=')
		x = lex.EatId()
	}

	t.Logf("x = %s, y = %d", x, y)

	t.Error()

	// if err := scanner.Err(); err != nil {
	// 	fmt.Fprintln(os.Stderr, "reading standard input:", err)
	// }
}
