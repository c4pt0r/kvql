# kvql

[![GoDoc](https://pkg.go.dev/badge/github.com/c4pt0r/kvql?utm_source=godoc)](https://pkg.go.dev/github.com/c4pt0r/kvql)

A SQL-like query language on general Key-Value DB

## Syntax

Basic Types:

```
Number: number such as integer or float

String: string around by ', ", \`,

Boolean: true or false
```

Select Statement:

```
SelectStmt ::= "SELECT" Fields "WHERE" WhereConditions ("ORDER" "BY" OrderByFields)? ("GROUP" "BY" GroupByFields)? ("LIMIT" LimitParameter)?

Fields ::= Field (, Field)* |
           "*"

Field ::= Expression ("AS" FieldName)?

FieldName ::= String

OrderByFields ::= OrderByField (, OrderByField)*

OrderByField ::= FieldName ("ASC" | "DESC")*

GroupByFields ::= FieldName (, FieldName)*

LimitParameter ::= Number "," Number |
                   Number

WhereConditions ::= "!"? Expression

Expression ::= "("? BinaryExpression | UnaryExpression ")"?

UnaryExpression ::= KeyValueField | String | Number | Boolean | FunctionCall | FieldName

BinaryExpression ::= Expression Operator Expression |
                     Expression "BETWEEN" Expression "AND" Expression |
                     Expression "IN" "(" Expression (, Expression)* ")" |
                     Expression "IN" FunctionCall

Operator ::= MathOperator | CompareOperator | AndOrOperator

AndOrOperator ::= "&" | "|" | "AND" | "OR"

MathOperator ::= "+" | "-" | "*" | "/"

CompareOperator ::= "=" | "!=" | "^=" | "~=" | ">" | ">=" | "<" | "<="

KeyValueField ::= "KEY" | "VALUE"

FunctionCall ::= FunctionName "(" FunctionArgs ")" |
                 FunctionName "(" FunctionArgs ")" FieldAccessExpression*

FunctionName ::= String

FunctionArgs ::= FunctionArg ("," FunctionArg)*

FunctionArg ::= Expression

FieldAccessExpression ::= "[" String "]" |
                          "[" Number "]"
```

Put Statement:

```
PutStmt ::= "PUT" KVPair (, KVPair)*
KVPair ::= "(" Expression, Expression ")"
```

Delete Statement:

```
DeleteStmt ::= "DELETE" "WHERE" WhereConditions ("LIMIT" LimitParameter)?
```

Features:

1. Scan ranger optimize: EmptyResult, PrefixScan, RangeScan, MultiGet
2. Plan support Volcano model and Batch model
3. Expression constant folding
4. Support scalar function and aggregate function
5. Support hash aggregate plan
6. Support JSON and field access expression

## Known User

- [c4pt0r/tcli](https://github.com/c4pt0r/tcli) CLI tool for TiKV

## Examples:

```
# Simple query, get all the key-value pairs with key prefix 'k'
select * where key ^= 'k'

# Projection and complex condition
select key, int(value) + 1 where key in ('k1', 'k2', 'k3') & is_int(value)

# Aggregation query
select count(1), sum(int(value)) as sum, substr(key, 0, 2) as kprefix where key between 'k' and 'l' group by kprefix order by sum desc

# JSON access
select key, json(value)['x']['y'] where key ^= 'k' & int(json(value)['test']) >= 1
select key, json(value)['list'][1] where key ^= 'k'

# Filter by field name defined in select statement
select key, int(value) as f1 where f1 > 10
select key, split(value) as f1 where 'a' in f1
select key, value, l2_distance(list(1,2,3,4), json(value)) as l2_dis where key ^= 'embedding_json' & l2_dis > 0.6 order by l2_dis desc limit 5

# Put data
put ('k1', 'v1'), ('k2', upper('v' + key))

# Delete data by filter and limit delete rows
delete where key ^= 'prefix' and value ~= '^val_' limit 10
delete where key in ('k1', 'k2', 'k3')
```


## How to use this library

A full example: 

[https://github.com/c4pt0r/kvql/blob/master/examples/memkv/memkv.go](https://github.com/c4pt0r/kvql/blob/master/examples/memkv/memkv.go)


To get better error report, you can conver the error to `QueryBinder` and set the origin query like below:

```golang
...
opt := kvql.NewOptimizer(query)
plan, err := opt.BuildPlan(storage)
if err != nil {
	if qerr, ok := err.(kvql.QueryBinder); ok {
		qerr.BindQuery(query)
	}
	fmt.Printf("Error: %s\n", err.Error())
}
...
```

After bind the query to error it will output error result like:

```
padding                    query
v-----vv--------------------------------------------v
Error: select * where key ^= 'asdf' and val ^= 'test'          < query line
                                        ^--                    < error position
       Syntax Error: ^= operator with invalid left expression  < error message
```

About padding: user can use `kvql.DefaultErrorPadding` to change the default left padding spaces. Or can use `kvql.QueryBinder.SetPadding` function to change specify error's padding. The default padding is 7 space characters (length of `Error: `).

If you want to display the plan tree, like `EXPLAIN` statement in SQL, the `kvql.FinalPlan.Explain` function will return the plan tree in a string list, you can use below code to format the explain output:

```golang
...
opt := kvql.NewOptimizer(query)
plan, err := opt.BuildPlan(storage)
if err != nil {
	fatal(err)
}

output := ""
for i, plan := range plan.Explain() {
	padding := ""
	for x := 0; x < i*3; x++ {
		padding += " "
	}
	if i == 0 {
		output += fmt.Sprintf("%s%s\n", padding, plan)
	} else {
		output += fmt.Sprintf("%s`-%s\n", padding, plan)
	}
}
fmt.Println(output)
```

## Operators and Functions

### Operators

**Conparation operators**

* `=`: bytes level equals
* `!=`: bytes level not equals
* `^=`: prefix match
* `~=`: regexp match
* `>`: number or string greater than
* `>=`: number or string greater or equals than
* `<`: number or string less than
* `<=`: number or string less or equals than
* `BETWEEN x AND y`: great or equals than `x` and less or equals than `y`
* `IN (...)`: in list followed by `in` operator

**Logical operators**

* `&`, `AND`: logical and
* `|`, `OR`: logical or
* `!`: logical not

**Math operators**

* `+`: number add or string concate
* `-`: number subtraction
* `*`: number multiply
* `/`: number division

### Scalar Functions

| Function | Description |
| -------- | ----------- |
| lower(value: str): str | convert value string into lower case |
| upper(value: str): str | convert value string into upper case |
| int(value: any): int | convert value into integer, if cannot convert to integer just return error
| float(value: any): float | convert value into float, if cannot convert to float just return error |
| str(value: any): str | convert value into string |
| is_int(value: any): bool | return is value can be converted into integer |
| is_float(value: any): bool | return is value can be converted into float |
| substr(value: str, start: int, end: int): str | return substring of value from `start` position to `end` position |
| split(value: str, spliter: str): list | split value into a string list by spliter string |
| list(elem1: any, elem2: any...): list | convert many elements into a list, list elements' type must be same, the list type support `int`, `str`, `float` types |
| float_list(elem1: float, elem2: float...): list | convert many float elements into a list |
| flist(elem1: float, elem2: float...): list | same as float_list |
| int_list(elem1: int, elem2: int...): list | convert many integer elements into a list |
| ilist(elem1: int, elem2: int...): list | same as int_list |
| len(value: list): int | return value list length |
| l2_distance(left: list, right: list): float | calculate l2 distance of two list |
| cosine_distance(left: list, right: list): float | calculate cosine distance of two list |
| json(value: str): json | parse string value into json type |
| join(seperator: str, val1: any, val2: any...): str | join values by seperator |

### Aggregation Functions

| Function | Description |
| -------- | ----------- |
| count(value: int): int | Count value by group |
| sum(value: int): int | Sum value by group |
| avg(value: int): int | Calculate average value by group |
| min(value: int): int | Find the minimum value by group |
| max(value: int): int | Find the maxmum value by group |
| quantile(value: float, percent: float): float | Calculate the Quantile by group |
