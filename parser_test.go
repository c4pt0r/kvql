package kvql

import (
	"fmt"
	"testing"
)

func TestParser1(t *testing.T) {
	query := "where key = 'test' & value = 'value'"
	p := NewParser(query)
	expr, err := p.Parse()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%+v\n", expr.Where.Expr.String())
}

func TestParser2(t *testing.T) {
	query := "where key ^= 'test'"
	p := NewParser(query)
	expr, err := p.Parse()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%+v\n", expr.Where.Expr.String())
}

func TestParser3(t *testing.T) {
	query := "where key ^= 'test' value = 'xxx'"
	p := NewParser(query)
	expr, err := p.Parse()
	if err == nil {
		fmt.Printf("%+v\n", expr.Where.Expr.String())
		t.Fatal("Should get syntax error")
	}
	fmt.Printf("%+v\n", err)
}

func TestParser4(t *testing.T) {
	query := "where (key ^= 'test' | key ^= 'bar') & value = 'xxx'"
	p := NewParser(query)
	expr, err := p.Parse()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%+v\n", expr.Where.Expr.String())
}

func TestParser5(t *testing.T) {
	query := "where (key ^= 'test' | (key ^= 'bar' & key ^= 'foo')) & value = 'xxx'"
	p := NewParser(query)
	expr, err := p.Parse()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%+v\n", expr.Where.Expr.String())
}

func TestParser6(t *testing.T) {
	query := "where !(key ^= 'test' | !(key ^= 'bar' & key ^= 'foo')) & value = 'xxx'"
	p := NewParser(query)
	expr, err := p.Parse()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%+v\n", expr.Where.Expr.String())
}

func TestParser7(t *testing.T) {
	funcMap["func_name"] = &Function{"func_name", 2, false, TBOOL, nil, nil}
	query := "where func_name(key, 'test')"
	p := NewParser(query)
	expr, err := p.Parse()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%+v\n", expr.Where.Expr.String())
}

func TestParser8(t *testing.T) {
	funcMap["func_name"] = &Function{"func_name", 2, false, TSTR, nil, nil}
	query := "where func_name(key, 'test') ^= 'name'"
	p := NewParser(query)
	expr, err := p.Parse()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%+v\n", expr.Where.Expr.String())
}

func TestParser9(t *testing.T) {
	funcMap["func_name"] = &Function{"func_name", 2, false, TSTR, nil, nil}
	funcMap["func_name2"] = &Function{"func_name2", 1, false, TBOOL, nil, nil}
	query := "where (func_name(key, 'test') ^= 'name') & (func_name2(value) | value ^= 't')"
	p := NewParser(query)
	expr, err := p.Parse()
	if err != nil {
		fmt.Println(err)
		t.Fatal(err)
	}
	fmt.Printf("%+v\n", expr.Where.Expr.String())
}

func TestParser10(t *testing.T) {
	funcMap["func1"] = &Function{"func1", 2, false, TBOOL, nil, nil}
	query := "where func1(func2(key), '')"
	p := NewParser(query)
	expr, err := p.Parse()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%+v\n", expr.Where.Expr.String())
}

func TestParser11(t *testing.T) {
	funcMap["func1"] = &Function{"func1", 2, false, TBOOL, nil, nil}
	query := "where func1(func2(key), '', func3(func4('1', '2'), '5'))"
	p := NewParser(query)
	expr, err := p.Parse()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%+v\n", expr.Where.Expr.String())
}

func TestParser12(t *testing.T) {
	funcMap["func1"] = &Function{"func1", 2, false, TBOOL, nil, nil}
	query := "where func1(func2(key), func3(func4('1', '2'), '5'), func5())"
	p := NewParser(query)
	expr, err := p.Parse()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%+v\n", expr.Where.Expr.String())
}

func TestParser13(t *testing.T) {
	funcMap["func1"] = &Function{"func1", 2, false, TBOOL, nil, nil}
	query := "where func1(key, func2(), (key = 'test'))"
	p := NewParser(query)
	expr, err := p.Parse()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%+v\n", expr.Where.Expr.String())
}

func TestParser14(t *testing.T) {
	query := "select * where key = '1'"
	p := NewParser(query)
	expr, err := p.Parse()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%+v\n", expr.Where.Expr.String())
	fmt.Printf("%+v\n", *expr)
}

func TestParser15(t *testing.T) {
	query := "select key, int(value) where str(int(key) + 1) = '1'"
	p := NewParser(query)
	expr, err := p.Parse()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%+v\n", expr.Where.Expr.String())
	fmt.Printf("%+v\n", *expr)
}

func TestParser16(t *testing.T) {
	query := "select key, int(value) where int(key) + 1 >= 1 & (int(value) - 1 > 10 | int(value) <= 20)"
	p := NewParser(query)
	expr, err := p.Parse()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%+v\n", expr.Where.Expr.String())
	fmt.Printf("%+v\n", *expr)
}

func TestParser17(t *testing.T) {
	query := "select key, int(value) where key ^= 'key' limit 10"
	p := NewParser(query)
	expr, err := p.Parse()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%+v\n", expr.Where.Expr.String())
	fmt.Printf("%+v\n", *expr.Limit)
	fmt.Printf("%+v\n", *expr)
}

func TestParser18(t *testing.T) {
	query := "select key, int(value) where key ^= 'key' limit 20, 10"
	p := NewParser(query)
	expr, err := p.Parse()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%+v\n", expr.Where.Expr.String())
	fmt.Printf("%+v\n", *expr.Limit)
	fmt.Printf("%+v\n", *expr)
}

func TestParser19(t *testing.T) {
	query := "select key, int(value) where key ^= 'key' order by key limit 20, 10"
	p := NewParser(query)
	expr, err := p.Parse()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%+v\n", expr.Where.Expr.String())
	fmt.Printf("%+v\n", *expr.Order)
	fmt.Printf("%+v\n", *expr.Limit)
	fmt.Printf("%+v\n", *expr)
}

func TestParser20(t *testing.T) {
	query := "select key, int(value), value where key ^= 'key' order by key, value desc limit 20, 10"
	p := NewParser(query)
	expr, err := p.Parse()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%+v\n", expr.Where.Expr.String())
	fmt.Printf("%+v\n", *expr.Order)
	fmt.Printf("%+v\n", *expr.Limit)
	fmt.Printf("%+v\n", *expr)
}

func TestParser21(t *testing.T) {
	query := "select * where key in ('k1', 'k2', 'k3')"
	p := NewParser(query)
	expr, err := p.Parse()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%+v\n", expr.Where.Expr.String())
}

func TestParser22(t *testing.T) {
	query := "select * where key between 'k1' and 'k3'"
	p := NewParser(query)
	expr, err := p.Parse()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%+v\n", expr.Where.Expr.String())
}

func TestParser23(t *testing.T) {
	query := "select * where key between 'k1' and 'k3' & int(value) between 1 and 10"
	p := NewParser(query)
	expr, err := p.Parse()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%+v\n", expr.Where.Expr.String())

}

func TestParser24(t *testing.T) {
	query := "select * where (key between 'k1' and 'k3') & int(value) between 1 and 10"
	p := NewParser(query)
	expr, err := p.Parse()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%+v\n", expr.Where.Expr.String())
}

func TestParser25(t *testing.T) {
	query := "select key, json(value)['test'] where key ^= 'k' & json(value)['test'] = 'v1'"
	p := NewParser(query)
	expr, err := p.Parse()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%+v\n", expr.Where.Expr.String())
}

func TestParser26(t *testing.T) {
	query := "select key, json(value)['test'] where key ^= 'k' & json(value)['test'][1] = 'v1'"
	p := NewParser(query)
	expr, err := p.Parse()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%+v\n", expr.Where.Expr.String())
}

func TestParser27(t *testing.T) {
	query := "select key, json(value)[1] where key ^= 'k' & json(value)['test'][1] = 'v1'"
	p := NewParser(query)
	expr, err := p.Parse()
	if err == nil {
		t.Fatal("Require error")
	}
	fmt.Printf("%+v\n", expr.Where.Expr.String())
}

func TestParser28(t *testing.T) {
	query := "select key, split(key, '_')[1], split(key, '_')[2] where key ^= 'k' & json(value)['test'][1] = 'v1'"
	p := NewParser(query)
	expr, err := p.Parse()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%+v\n", expr.Where.Expr.String())
}

func TestParser29(t *testing.T) {
	query := "select key, int(split(key, '_')[1]) as f2, split(key, '_')[2] as f3 where key ^= 'k' & f2 > 10"
	p := NewParser(query)
	expr, err := p.Parse()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%+v\n", expr.Where.Expr.String())
}
