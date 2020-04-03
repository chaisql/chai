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
          "favorite game": "FF IX"
        }
    ]
}
```

Accessing a top-level field can be achieved by simply referring to its name.

_Example_: `name` will evaluate to `"Foo"`.

To access a nested field, concatenate all the fields with the `.` character.

_Examples_: `address.city` will evaluate to `"Lyon"` 

To access an element of an array, use the index of the element

_Examples_:

* `friends.0` will evaluate to `{"name": "Bar","address": {"city":"Paris","zipcode": "75001"}}`
* `friends.1.name` will evaluate to `"Baz"`
* `friends.1."favorite game"` will evaluate to `"ffix"`
