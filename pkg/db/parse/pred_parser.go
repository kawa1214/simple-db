package parse

type PredParser struct {
	lex *Lexer
}

func NewPredParser(s string) *PredParser {
	return &PredParser{lex: NewLexer(s)}
}

func (p *PredParser) Field() string {
	return p.lex.EatId()
}

func (p *PredParser) Constant() {
	if p.lex.MatchStringConstant() {
		p.lex.EatStringConstant()
	} else {
		p.lex.EatIntConstant()
	}
}

func (p *PredParser) Expression() {
	if p.lex.MatchId() {
		p.Field()
	} else {
		p.Constant()
	}
}

func (p *PredParser) Term() {
	p.Expression()
	p.lex.EatDelim('=')
	p.Expression()
}

func (p *PredParser) Predicate() {
	p.Term()
	if p.lex.MatchKeyword("and") {
		p.lex.EatKeyword("and")
		p.Predicate()
	}
}
