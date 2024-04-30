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

Remove Statement:

```
RemoveStmt ::= "REMOVE" Expression (, Expression)*
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
```


## How to use this library

```
```
