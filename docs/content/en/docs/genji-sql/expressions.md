---
title: "Expressions"
date: 2020-04-03T12:36:25+04:00
weight: 40
description: >
    How expression are evaluated, compared, etc.
---

## Expressions

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

### Literal expressions

Any [literal](#literals) evaluates to itself

```python
1
-> 1

"hello"
-> "hello"
```

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