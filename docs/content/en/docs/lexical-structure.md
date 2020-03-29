---
title: "Lexical Structure"
date: 2020-03-29T17:48:51+04:00
weight: 3
description: >
    Describes of how SQL components are parsed by Genji.
---

## Identifiers

Identifiers are sequence of characters which refer to table names, field names and index names.

Identifiers may be unquoted or surrounded by backquotes. Depending on that, different rules may apply.

<table>
  <thead>
    <tr>
      <th style="text-align:left">Unquoted identifiers</th>
      <th style="text-align:left">Identifiers surrounded by backquotes </th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align:left">
        <p></p>
        <ul>
          <li>Must begin with an uppercase or lowercase ASCII character or an underscore</li>
          <li>May contain only ASCII letters, digits and underscore</li>
        </ul>
      </td>
      <td style="text-align:left">
        <p></p>
        <ul>
          <li>May contain any unicode character, other than the new line character (i.e. <code>\n</code>)</li>
          <li>May contain escaped <code>`</code> character (i.e. <code>\`</code>)</li>
        </ul>
      </td>
    </tr>
  </tbody>
</table>

```text
foo
_foo_123_
`頂きます (*｀▽´)_旦~~`
`foo \` bar`
```

## Literals

### Strings

A string is a sequence of characters surrounded by double or single quotes. They may contain any unicode character or escaped single or double quotes \(i.e `\'` or `\"`\)

```sql
foo
"l'école des fans"
'(╯ಠ_ಠ）╯︵ ┳━┳'
'foo \''
```

### Integers

An integer is a sequence of characters that only contain digits. They may start with a `+` or `-`sign.

```sql
123456789
+100
-455
```

### Floats

A float is a sequence of characters that contains three parts:

- a sequence of digits
- a decimal point \(i.e. `.`\)
- a sequence of digits

They may start with a `+`or a `-`sign.

```sql
123.456
+3.14
-1.0
```

### Booleans

A boolean is any sequence of character that is written as `true` or `false`, regardless of the case.

```sql
true
false
TRUE
FALSE
tRUe
FALse
```

### Arrays

An array is any sequence of character that starts and ends with either:

- `(` and `)`
- `[` and `]`

and that contains a coma-separated list of expressions.

```python
[1.5, "hello", 1 > 10, [true, -10], {foo: "bar"}]
```

### Documents

A document is any sequence of character that starts and ends with `{` and `}` and that contains a list of pairs.
Each pair associates an identifier with an expression, both separated by a colon. Each pair must be separated by a coma.

```js
{
  foo: 1,
  bar: "hello",
  baz: true AND false,
  "long field": {
    a: 10
  }
}
```

In a document, the identifiers are refered to as **fields**.
In the example above, the document has four top-level fields (`foo`, `bar`, `baz` and `long field`) and one nested field `a`.

## Dot notation

The _dot notation_ is a way to refer to fields of a document or elements of an array.

Given the following document:

```js
{
  "name": "Foo",
  "address": {
    "city": "Lyon",
    "zipcode": "69001"
  },
  "friends": [
    {
      "name": "Bar",
      "address": {
        "city": "Paris",
        "zipcode": "75001"
      }
    },
    {
      "name": "Baz",
      "address": {
        "city": "Ajaccio",
        "zipcode": "20000"
      },
      "favorite game": "ffix"
    }
  ]
}
```

Accessing a top-level field can be achieved by simply refering to its name.

_Example_: `name` will evaluate to `"Foo"`.

To access a nested field, concatenate all the fields with the `.` character.

_Examples_: `address.city` will evaluate to `"Lyon"`

To access an element of an array, use the index of the element

_Examples_:

- `friends.0` will evaluate to `{"name": "Bar","address": {"city":"Paris","zipcode": "75001"}}`
- `friends.1.name` will evaluate to `"Baz"`
- `friends.1."favorite game"` will evaluate to `"ffix"`

## Expressions

Expressions are components that can be evaluated to a value.

Example:

```python
1 + 1
-> 2
```

An expression can be found in two forms:

- unary: meaning it contains only one component
- binary: meaning it contains two expressions, or operands, and one operator. i.e. `<expr> <operator> <expr>`

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

Currently, Genji only supports comparison operators:

| Name | Description                                                                                                                      |
| :--- | :------------------------------------------------------------------------------------------------------------------------------- |
| =    | Evaluates to `true` if operands are equal, otherwise returns `false`                                                             |
| !=   | Evaluates to `true` if operands are not equal, otherwise returns `false`                                                         |
| >    | Evaluates to `true` if the left-side expression is greater than the right-side expression, otherwise returns `false`             |
| >=   | Evaluates to `true` if the left-side expression is greater than or equal to the right-side expression, otherwise returns `false` |
| <    | Evaluates to `true` if the left-side expression is less than the right-side expression, otherwise returns `false`                |
| <=   | Evaluates to `true` if the left-side expression is less than or equal to the right-side expression, otherwise returns `false`    |

Examples:

```python
1 = 1
-> true

1 > 2.5
-> false
```

Comparison between type is described in [this page](data-types.md#conversion).
