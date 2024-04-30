# kvql

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

UnaryExpression ::= KeyValueField | String | Number | Boolean | FunctionCall

BinaryExpression ::= Expression Operator Expression |
                     Expression "BETWEEN" Expression "AND" Expression |
                     Expression "IN" "(" Expression (, Expression)* ")" |
                     Expression "IN" FunctionCall

Operator ::= MathOperator | CompareOperator | AndOrOperator

AndOrOperator ::= "&" | "AND" | "|" | "OR"

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

Remove Statement:

```
RemoveStmt ::= "REMOVE" Expression (, Expression)*
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

## Examples:

```
# Simple query
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

# Remove data
remove 'k1', 'k2'

# Delete data by filter and limit delete rows
delete where key ^= 'prefix' and value ~= '^val_' limit 10
```


## How to use this library

First implements interfaces defined in `kv.go`:

```
type Txn interface {
	Get(key []byte) (value []byte, err error)
	Put(key []byte, value []byte) error
	BatchPut(kvs []KVPair) error
	Delete(key []byte) error
	BatchDelete(keys [][]byte) error
	Cursor() (cursor Cursor, err error)
}

type Cursor interface {
	Seek(prefix []byte) error
	Next() (key []byte, value []byte, err error)
}
```

Then execute query:

```
var (
    query string = "select * where key ^= 'k'"
    txn kvql.Txn = buildClientTxn()
)

optimizer := kvql.NewOptimizer(query)
plan, err := opt.BuildPlan(txn)
if err != nil {
    fatal(err)
}

execCtx := kvql.NewExecuteCtx()
for {
    rows, err := plan.Batch(execCtx)
    if err != nil {
        fatal(err)
    }
    if len(rows) == 0 {
        break
    }
    execCtx.Clear()
    for _, cols := range rows {
        // Process columns...
    }
}
```
