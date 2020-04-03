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

Any [literal]({{< relref "/docs/genji-sql/lexical-structure" >}}#literals) evaluates to the closest compatible [type]({{< relref "/docs/genji-sql/data-types" >}}#fixed-size-data-types.

### Strings

[Strings]({{< relref "/docs/genji-sql/lexical-structure" >}}#literals) are evaluated to the [`text` type]({{< relref "/docs/genji-sql/data-types" >}}#variable-size-data-types), which are utf-8 encoded.

### Integers

[Integers]({{< relref "/docs/genji-sql/lexical-structure" >}}#integers) are evaluated into the smallest [integer type]({{< relref "/docs/genji-sql/data-types" >}}#fixed-size-data-types) that can contain the value.

Example:

* `10` will be evaluated to an `int8`, because is it bigger than -128 and smaller than 127
* `-500` will be evaluated to an `int16`, because it is smaller than -128 but bigger than -32768

If an integer is bigger than the maximum `int64` value or smaller than the minimum `int64` value, it will be evaluated as a `float64`.

### Floats

[Floats]({{< relref "/docs/genji-sql/lexical-structure" >}}#floats) are evaluated into the [`float64` type]({{< relref "/docs/genji-sql/data-types" >}}#fixed-size-data-types).

### Booleans

[Booleans]({{< relref "/docs/genji-sql/lexical-structure" >}}#booleans) are evaluated into the [`bool` type]({{< relref "/docs/genji-sql/data-types" >}}#fixed-size-data-types).

### Dot notation

[Dot notation]({{< relref "/docs/genji-sql/lexical-structure" >}}#dot-notation) are evaluated into the value they refer to.
They are used to select a value from a [document]({{< relref "/docs/genji-sql/documents" >}}).
Their type will depend on the type of the value extracted from the document.

### Operators

Genji provides a list of operators that can be used to compute operations with expressions.

#### Comparison operators

These operators are used to compare values and evaluate to a boolean. If the types of the operands are different, they are converted before being evaluated.

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

#### Converting numbers

Let `a` and `b` two numbers.

1. If one of them is a `float64`, the other one is converted to `float64`
2. If one of them 