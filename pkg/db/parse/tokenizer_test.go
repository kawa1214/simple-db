package parse

import (
	"strconv"
	"strings"
	"testing"
	"text/scanner"
)

var keywords = map[string]bool{
	"select": true, "from": true, "where": true, "and": true,
	"insert": true, "into": true, "values": true, "delete": true,
	"update": true, "set": true, "create": true, "table": true,
	"int": true, "varchar": true, "view": true, "as": true,
	"index": true, "on": true,
}

func TestTokenizer(t *testing.T) {
	s := getStringFromUser()
	var scan scanner.Scanner
	scan.Init(strings.NewReader(s))
	scan.Mode = scanner.ScanInts | scanner.ScanStrings | scanner.ScanIdents
	for tok := scan.Scan(); tok != scanner.EOF; tok = scan.Scan() {
		printCurrentToken(t, &scan)
	}
	t.Error()
}

func getStringFromUser() string {
	return "select * from table1 where a = 1 and b = 'hello'"
}

func printCurrentToken(t *testing.T, scan *scanner.Scanner) {
	tok := scan.TokenText()
	switch {
	case scan.TokenText() == ".":
		t.Logf("Delimiter, %v", tok)
	case scan.TokenText()[0] == '\'':
		t.Logf("StringConstant, %v", tok)
	case keywords[tok]:
		t.Logf("Keyword, %v", tok)
	default:
		if _, err := strconv.Atoi(tok); err == nil {
			t.Logf("IntConstant, %v", tok)
		} else {
			t.Logf("Id, %v", tok)
		}
	}
}
