package kvql

import (
	"fmt"
	"testing"
)

func TestLexer1(t *testing.T) {
	query := "where key = 'test' & value = 'value'"
	l := NewLexer(query)
	toks := l.Split()
	for _, t := range toks {
		fmt.Printf("%s\n", t.String())
	}
}

func TestLexer2(t *testing.T) {
	query := "where key ^= 'test' | key ~= 'value' & value = 'test'"
	l := NewLexer(query)
	toks := l.Split()
	for _, t := range toks {
		fmt.Printf("%s\n", t.String())
	}
}

func TestLexer3(t *testing.T) {
	query := "where key^='test'|key~='value'&value='test' "
	l := NewLexer(query)
	toks := l.Split()
	for _, t := range toks {
		fmt.Printf("%s\n", t.String())
	}
}

func TestLexer4(t *testing.T) {
	query := "where key^='test'|(key~='value'&value='test')"
	l := NewLexer(query)
	toks := l.Split()
	for _, t := range toks {
		fmt.Printf("%s\n", t.String())
	}
}

func TestLexer5(t *testing.T) {
	query := "where !(key^='test')"
	l := NewLexer(query)
	toks := l.Split()
	for _, t := range toks {
		fmt.Printf("%s\n", t.String())
	}
}

func TestLexer6(t *testing.T) {
	query := "where func_name(key, 'test')"
	l := NewLexer(query)
	toks := l.Split()
	for _, t := range toks {
		fmt.Printf("%s\n", t.String())
	}
}

func TestLexer7(t *testing.T) {
	query := "select * where func_name(key, 'test')"
	l := NewLexer(query)
	toks := l.Split()
	for _, t := range toks {
		fmt.Printf("%s\n", t.String())
	}
}

func TestLexer8(t *testing.T) {
	query := "select * where int(key) + 10"
	l := NewLexer(query)
	toks := l.Split()
	for _, t := range toks {
		fmt.Printf("%s\n", t.String())
	}
}

func TestLexer9(t *testing.T) {
	query := "select * where int(key) + 10.5"
	l := NewLexer(query)
	toks := l.Split()
	for _, t := range toks {
		fmt.Printf("%s\n", t.String())
	}
}

func TestLexer10(t *testing.T) {
	query := "select * where int(key) + 10.5.7"
	l := NewLexer(query)
	toks := l.Split()
	for _, t := range toks {
		fmt.Printf("%s\n", t.String())
	}
}

func TestLexer11(t *testing.T) {
	query := "select * where int(key) + 10 > 5 & int(value) - 10 < 8"
	l := NewLexer(query)
	toks := l.Split()
	for _, t := range toks {
		fmt.Printf("%s\n", t.String())
	}
}

func TestLexer12(t *testing.T) {
	query := "select * where key ^= 'asdf\"jkl'"
	l := NewLexer(query)
	toks := l.Split()
	for _, t := range toks {
		fmt.Printf("%s\n", t.String())
	}
}

func TestLexer13(t *testing.T) {
	query := "select * where key ^= 'asdf' order by key"
	l := NewLexer(query)
	toks := l.Split()
	for _, t := range toks {
		fmt.Printf("%s\n", t.String())
	}
}

func TestLexer14(t *testing.T) {
	query := "select * where key ^= 'asdf' order by key asc"
	l := NewLexer(query)
	toks := l.Split()
	for _, t := range toks {
		fmt.Printf("%s\n", t.String())
	}
}

func TestLexer15(t *testing.T) {
	query := "select * where key ^= 'asdf' order by key desc"
	l := NewLexer(query)
	toks := l.Split()
	for _, t := range toks {
		fmt.Printf("%s\n", t.String())
	}
}

func TestLexer16(t *testing.T) {
	query := "insert ('k1', 'v1')"
	l := NewLexer(query)
	toks := l.Split()
	for _, t := range toks {
		fmt.Printf("%s\n", t.String())
	}
}

func TestLexer17(t *testing.T) {
	query := "insert ('k1', 'v1'), ('k2', 'v2'), ('k3', upper('v3'))"
	l := NewLexer(query)
	toks := l.Split()
	for _, t := range toks {
		fmt.Printf("%s\n", t.String())
	}
}

func TestLexer18(t *testing.T) {
	query := "remove 'k1', 'k2'"
	l := NewLexer(query)
	toks := l.Split()
	for _, t := range toks {
		fmt.Printf("%s\n", t.String())
	}
}

func TestLexer19(t *testing.T) {
	query := "delete where key ^= 'kp' and value ^= 'v_' "
	l := NewLexer(query)
	toks := l.Split()
	for _, t := range toks {
		fmt.Printf("%s\n", t.String())
	}
}
