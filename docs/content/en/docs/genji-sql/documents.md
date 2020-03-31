---
title: "Documents"
date: 2020-03-31T16:51:59+04:00
weight: 15
description: >
  Description of documents
---

Genji stores records as documents. A document is an object that contains pairs that associate a string field to a value of any type.
Genji SQL represents documents as JSON objects, though they support far more types.

Here is a JSON representation of the structure of a document:

```js
{
    field1: value1,
    field2: value2,
    field3: value3,
    ...
}
```

Example of a document using Genji SQL syntax:

```js
{
    name: "Nintendo Switch",
    price: {
        base: 379.99,
        vat: 20,
        total: base + base * vat / 100
    },
    brand: "Nintendo",
    "top-selling-games": [
        "Mario Odyssey",
        "Zelda Breath of the Wild"
    ]
}
```

Each field name must be a string, but values can be of any type, including another document, an array or an expression.

Any JSON object is a valid document and can be inserted as-is.

## Field names

Field names can be any string, with only one exception: they cannot be empty.

## Dot notation

To access an element of a document or an array, Genji uses the *dot notation*.

For accessing a top-level field of a document, simply write its name. If the name contains [spaces or special characters]({{< relref "/docs/genji-sql/lexical-structure.md" >}}), enclose it with backquotes:

```sql
foo
`foo bar`
```

For accessing a field of a nested document, concatenate both field names with a dot `.`:

```sql
foo.bar
`foo bar`.baz
foo.`bar baz`
`foo bar`.`baz bat`
foo.bar.baz
```

For accessing the value of an array, concatenate the field name whose value is an array with a dot and the numeric index.

```sql
foo.0 /* accessing the first element of an array foo */
foo.5
`foo bar`.3
```

For accessing a deeply nested value, combine for approaches:

```sql
foo.bar.4.`baz bat`.3
```
