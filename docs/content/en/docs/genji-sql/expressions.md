---
title: "Expressions"
date: 2020-04-03T12:36:25+04:00
weight: 40
description: >
    How expression are evaluated, compared, etc.
---

Expressions are components that can be evaluated to a value.

Example:

```python
1 + 1 # expression
-> 2  # result
```

An expression can be found in two forms:

* **unary**: meaning it contains only one component
* **binary**: meaning it contains two expressions, or *operands*, and one *operator*. i.e. `<expr> <operator> <expr>`

Example:

```sql
/* Unary expressions */
1
name
"foo"

/* Binary expressions */
age >= 18
1 AND 0
```

Here is a list of expressions supported by Genji.

## Literal expressions

Any [literal]({{< relref "/docs/genji-sql/lexical-structure" >}}#literals) evaluates to the closest compatible [type]({{< relref "/docs/genji-sql/data-types" >}}#fixed-size-data-types).

### Strings

[Strings]({{< relref "/docs/genji-sql/lexical-structure" >}}#literals) are evaluated to the [`text`]({{< relref "/docs/genji-sql/data-types" >}}#variable-size-data-types) type, which are utf-8 encoded.

### Integers

[Integers]({{< relref "/docs/genji-sql/lexical-structure" >}}#integers) are evaluated into the smallest [integer]({{< relref "/docs/genji-sql/data-types" >}}#fixed-size-data-types) type that can contain the value.

Example:

* `10` will be evaluated to an `int8`, because is it bigger than -128 and smaller than 127
* `-500` will be evaluated to an `int16`, because it is smaller than -128 but bigger than -32768

If an integer is bigger than the maximum `int64` value or smaller than the minimum `int64` value, it will be evaluated as a `float64`.

### Floats

[Floats]({{< relref "/docs/genji-sql/lexical-structure" >}}#floats) are evaluated into the [`float64`]({{< relref "/docs/genji-sql/data-types" >}}#fixed-size-data-types) type.

### Booleans

[Booleans]({{< relref "/docs/genji-sql/lexical-structure" >}}#booleans) are evaluated into the [`bool`]({{< relref "/docs/genji-sql/data-types" >}}#fixed-size-data-types) type.

### Dot notation

[Dot notation]({{< relref "/docs/genji-sql/lexical-structure" >}}#dot-notation) are evaluated into the value they refer to.
They are used to select a value from a [document]({{< relref "/docs/genji-sql/documents" >}}).
Their type will depend on the type of the value extracted from the document.

## Operators

Genji provides a list of operators that can be used to compute operations with expressions.
Operators are binary expressions, meaning they always take exactly two operands.
It is possible though to combine multiple operators to create an [evaluation tree](#evaluation-tree-and-precedence).

### Comparison operators

These operators are used to compare values and evaluate to a boolean.

| Name| Description |
| --- | --- |
| =    | Evaluates to `true` if operands are equal, otherwise returns `false` |
| !=   | Evaluates to `true` if operands are not equal, otherwise returns `false` |
| >    | Evaluates to `true` if the left-side expression is greater than the right-side expression, otherwise returns `false` |
| >=   | Evaluates to `true` if the left-side expression is greater than or equal to the right-side expression, otherwise returns `false` |
| <    | Evaluates to `true` if the left-side expression is less than the right-side expression, otherwise returns `false` |
| <=   | Evaluates to `true` if the left-side expression is less than or equal to the right-side expression, otherwise returns `false` |

Examples:

```python
1 = 1
-> true

1 > 2.5
-> false
```

#### Conversion during comparison

Prior to comparison, an implicit conversion is operated for the operands to be of the same type.
Not all types can be compared together. When two incompatible types are compared, the comparison always returns `false`.

Example:

```python
1 > "hello"
-> false
```

```python
1 < "hello"
-> false
```

The comparison follows a list of rules that are executed in order:

* If one of the operands is NULL, use the [Comparing with NULL](#comparing-with-null) rule
* If both operands are documents, use the [Comparing documents](#comparing-documents) rule
* If both operands are arrays, use the [Comparing arrays](#comparing-arrays) rule
* If one of the operands is a boolean, use the [Comparing with boolean](#comparing-with-a-boolean) rule
* If both operands are either text or blob, compare them byte per byte
* If both operands are integers, compare them together
* If both operands are numbers (integer or float), convert them to 64 float then compare them together.

In any other case, return `false`.

#### Comparing with NULL

Any comparison with NULL will return `false`, except in the following cases:

* `NULL = NULL`
* `NULL >= NULL`
* `NULL <= NULL`
* Using the `!=` operator with a value other than NULL will return `true`. (Ex: `1 != null`, `foo != NULL`, etc.)

#### Comparing documents

Only the `=` operator is supported when comparing documents.

A document is equal to another document if all of the following conditions are verified:

* it has the same number of fields
* all the fields of the first document are present in the other document
* every field of the first document is equal to the same field in the other document

```python
{a: 1, b: 2} = {b: 2, a: 1}
-> true

{} = {}
-> true
```

#### Comparing arrays

Each elements of both arrays are compared one by one, index by index, until they are found not equal. The comparison is then determined by the result of the comparison between these two values.

```python
[1, 2, 3] > [1, 1 + 1, 1]
-> true
```

Let's break down the example above:

1. Index 0: `1` and `1` are equal, the comparison continues
2. Index 1: `2` and `1 + 1` are equal, the comparison continues
3. Index 2: `3` is greater then `1`, the comparison stops and the first array is considered greater than the second one

Two empty arrays are considered equal:

```python
[] = []
-> true
```

The size of arrays doesn't matter, unless all the elements of the smallest one are equal to the other one. In that case the biggest array is considered greater.

```python
[3] > [1, 100000]
-> true

[1, 2] < [1, 2, 3]
-> true
```

#### Comparing with a boolean

When comparing booleans together, there is a simple rule: `true` is greater than `false`.

```python
true > false
-> true

false < true
-> true
```

If an operand is a boolean, but the other one is not, the other operand will be converted to a boolean, following these rules:

* `TEXT`: a non-empty text is equal to `true`, otherwise `false`
* `BLOB`: a non-empty blob is equal to `true`, otherwise `false`
* any number: if the number is different than 0, convert to `true`, otherwise `false`
* document: if the document contains at least one field, convert to `true`, otherwise `false`
* array: if the array contains at least one value, convert to `true`, otherwise `false`

Examples:

```python
"foo" > false
-> true

"" = {} = [] = 0 = false
-> true
```

### Arithmetic operators

| Name| Description |
| --- | --- |
| `+`     | Adding two values |
| `-`   | Substracting two values |
| `*`    | Multiplying two values |
| `/`   | Dividing two values |
| `%`    | Find the remainder after division of one number by another |
| `&`    | Bitwise AND |
| `|`    | Bitwise OR |
| `^`    | Bitwise XOR |

Arithmetic operations are supported only for the following types:

* `integer`
* `int8`, `int16`, `int32`, `int64`
* `float64`
* `duration`
* `bool`

#### The case of NULL

Any arithmetic operation with one of the operand being `NULL` returns `NULL`.

```python
NULL + 1
-> NULL

5 * 10 - NULL
-> NULL
```

#### Division rules

The division obeys a few rules depending on the types of the operands:

* Dividing two integers, always result in an integer
* Dividing by zero, returns `NULL`

#### Conversion rules

When running an arithmetic operation on two values of different types, an implicit conversion occurs, following this set of rules:

* if one of the operands is a boolean, convert to an integer
* if one of the operands is a float, convert the other one to float

#### Return type and overflow

The type of the result of an operation doesn't necessarily match the type of the operands.

* The result of a float operation will always return a float
* The result of an integer operation will return the smallest integer type that can hold the return value, unless the return value is bigger than the maximum value of 64-bit integer. In that case, the return type will be a float

### Evaluation tree and precedence

When parsed, an expression is turned into an evaluation tree so it is possible to combine operators to form complex expressions.
The order in which these expressions are executed depends on the priority of the operator.

Here is the list of operators ordered by ascending precedence. Operators with higher precedence are executed before the ones with lower precedence

* `OR`
* `AND`
* `=`, `!=`, `<`, `<=`, `>`, `>=`
* `+`, `-`, `|`, `^`
* `*`, `/`, `%`, `&`

Example:

```sql
3 + 4 * 2 > 10 AND 2 - 2 = false
-> true
```

This expression can be represented as the following tree:

```text
.
└── AND
    ├── >
    │   ├── +
    │   │   ├── 3
    │   │   └── *
    │   │       ├── 4
    │   │       └── 2
    │   └── 10
    └── -
        ├── 2
        └── 2
```

The deepest branches will be executed first, recursively until reaching the root.
