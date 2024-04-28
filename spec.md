# A SQL like query language for TiKV

The query package provide a SQL like query language for user to do some search operation on TiKV's key-value pair.

## Query Syntax

### Basic syntax

```
select (field expression), (field expression)... where (filter expression) group by (group expression) order by (order expression) limit (start, counts)
``` 

**Field Expression**

```
Field Expression := (FunctionCall | key | value | "*") ("as" FieldName)?

FunctionCall := FunctionName "(" FuncArgs ")" |
				   FunctionName "(" FuncArgs ")" FieldAccessExpression*

FuncArgs := Expression (, Expression)*

FieldAccessExpression := "[" string "]" | "[" number "]"
```

Basically can be `*`, `key` or `value` and you can use AS keyword to rename it. Such as:

```
# same as select key, value where key ^= "prefix"
select * where key ^= "prefix"

# rename key to f1 and value to f2 in result set
select key as f1, value as f2 where key ^= "prefix"
```

The `key` is key-value pair's key, and aslo `value` is the value.

If using function it support functions shows below:

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

You can use any of the functions above in field expression, such as:

```
# Convert value into int type
select key, int(value) where key ^= "prefix"

# Convert value into int type and do some math on it
select key, ((int(value) + 1) * 8) where key ^= "prefix"

# Convert value into upper case
select key, upper(value) where key ^= "prefix"

# Calculate l2 distance on two vectors
select key, l2_distance(list(1,2,3,4), split(value, ",")) where key ^= "prefix"
```

And you may notice there has a `json` type and yes you can use `[]` operator to access `json` map and list. And `[]` operator can also use in `list` type.

```
select key, json(value)["key1"]["key2"] where key ^= "prefix"

select key, list(1,2,3,4)[2] where key ^= "prefix"
```

**Filter Expression**

Filter expression followed the `where` keyword, and it contains filter condition expressions.

```
Filter Expression := "!"? Expression

Expression := "(" BinaryExpression | UnaryExpression ")"

UnaryExpression := "key" | "value" | string | number | "true" | "false" | FunctionCall | FieldName

BinaryExpression := Expression Op Expression |
						Expression "between" Expression "and" Expression |
						Expression "in" "(" Expression (, Expression)* ")" |
						Expression "in" FunctionCall |
						FunctionCall

Op := MathOp | CompareOp | AndOrOp
MathOp := "+" | "-" | "*" | "/"
AndOrOp := "&" | "|"
CompareOp := "=" | "!=" | "^=" | "~=" | ">" | ">=" | "<" | "<="

FunctionCall := FunctionName "(" FuncArgs ")" |
				   FunctionName "(" FuncArgs ")" FieldAccessExpression*

FuncArgs := Expression (, Expression)*

FieldAccessExpression := "[" string "]" | "[" number "]"
```

The basic usage of filter expression is filter key as equal or has same prefix. So there has some special compare operator for this:

* `=`: Equals
* `!=`: Not equals
* `^=`: Prefix match
* `~=`: Regexp match

For example:

```
# Key equals "key01"
select * where key = "key01"

# Keys that has "key01" prefix
select * where key ^= "key01"

# Keys that match "^key[0-9]+$"
select * where key ~= "^key[0-9]+$"
```

And we also provide `between` ... `and` expression and `in` expression same as SQL:

```
select * where key between "k" and "l"

select * where key in ("k1", "k2", "k3")
```

To concate more expressions you can use `&` and `|` operator:

```
select * where key in ("k1", "k2", "k3") & value ~= "^prefix[0-9]+"

select * where key ^= "key" | value ^= "val"
```

And then is using field name in filter expression, that will save some characters for SQL writer.

```
# filter value's substring from 2 to 3 (one char) is between "b" to "e"
select key, substr(value, 2, 3) as mid, value where mid between "b" and "e"
```

If you want, you can also do some math on filter expression:

```
select * where key ^= "num" & int(value) + 1 > 10
```

If value is a JSON string and you want to filter data by some fields, you can use field access operator:

```
select * where key ^= "json" & json(value)["user"] = "Bob"
```

**Order By**

Same as SQL, you can use `order by` to sort result set.

```
Order Expression := OrderByField (, OrderByField)*

OrderByField := FieldName (ASC | DESC)?
```

The `FieldName` can be `key`, `value` or the name defined by select:

```
select key, value where key ^= "prefix" order by value

select key, int(value) as snum where key ^= "prefix" order by snum asc, key asc
```

**Limit**

Same as SQL. If one number follow limit keyword just define how many rows return. If two numbers followed, first is how many rows should be skip and the second is how many rows return.

```
select * where key ^= "prefix" limit 10

select * where key ^= "prefix" limit 10, 10
```

### Aggregation

The query language also support aggregation. You can use `GROUP BY` expression like in SQL:

```
Group Expression := FieldName (, FieldName)*
```

Below is the aggregation function list:

| Function | Description |
| -------- | ----------- |
| count(value: int): int | Count value by group |
| sum(value: int): int | Sum value by group |
| avg(value: int): int | Calculate average value by group |
| min(value: int): int | Find the minimum value by group |
| max(value: int): int | Find the maxmum value by group |
| quantile(value: float, percent: float): float | Calculate the Quantile by group |

For example:

```
select count(1), substr(key, 3, 4) as pk where key ^= "k_" group by pk

select count(1), sum(int(value)) as sum, substr(key, 0, 2) as kprefix where key between 'k' and 'l' group by kprefix order by sum desc
```

### Put statement

If you want to insert some data into TiKV, you can use `put` statement.

```
PutStmt := "PUT" KeyValuePair (, KeyValuePair)*

KeyValuePair := "(" Expression "," Expression ")"
```

For example:

```
put ("k1", "v1"), ("k2", "v2")

# Use function call to generate value
put ("k3", upper("value3")), ("k4", join(",", 1, 2, 3, 4))

# use key keyword to generate value
put ("k4", upper("val_" + key))
```

Notice: In put statement you can only use `key` keyword to generate the value. If `value` keyword in statement it will report an syntax error.

### Remove statement

If you want to delete some data from TiKV, you ca use `remove` statement.

```
RemoveStmt := "REMOVE" Expression (, Expression)*
```

For example:

```
remove "k1", "k2"
```

Notice: In remove statement you cannot use `key` and `value` keyword.