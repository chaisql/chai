---
title: "Selecting Documents"
date: 2020-04-10T08:57:17+04:00
weight: 70
description: >
    How to use the SELECT statement to query documents in a table
---

Querying documents from a table can be achieved by using the `SELECT` statement.

In the Genji database, a *query* does two things:

- it reads documents from a table
- it uses the arguments of the query to transform, filter and project that data to create a *result*, which is a stream of documents.

This stream of documents can be consumed by the caller one by one, and each document will contain the fields the user chose.

Consider the following table:

```sql
CREATE TABLE users;
INSERT INTO users (name, age, nen, parents, abilities) VALUES
    ('Gon', 13, 'Enhancement', {'father': 'Ging Freecs'}, ['Jajanken']),
    (
        'Kirua', 14, 'Transmutation',
        {'father': 'Silva Zoldyck', 'mother': 'Kikyo Zoldyck'},
        ['Lighning Palm', 'Thunderbolt', 'Godspeed']
    );
INSERT INTO users (name, nen, abilities) VALUES
    ('Hisoka', 'Transmutation', ['Bungee Gum', 'Texture Surprise']);
```

## Querying all the documents

Selecting all users goes like this:

```sql
SELECT * FROM users;
```

```json
{
    "name": "Gon",
    "age": 13,
    "nen": "Enhancement",
    "parents": {
        "father": "Ging Freecs"
    },
    "abilities": ["Jajanken"]
}
{
    "name": "Kirua",
    "age": 14,
    "nen": "Transmutation",
    "parents": {
        "father": "Silva Zoldyck",
        "mother": "Kikyo Zoldyck"
    },
    "abilities": ["Lighning Palm", "Thunderbolt", "Godspeed"]
}
{
    "name": "Hisoka",
    "nen": "Transmutation",
    "abilities": ["Bungee Gum", "Texture Surprise"]
}
```

Let's break it down:

- `SELECT`: Run the SELECT command
- `*`: This is the *projection*, it indicates how to build the documents returned by the result of the query. Here, we are using a special projection called the *wildcard*, which is a way to tell Genji to simply project all of the fields of each document.
- `FROM users`: Indicates from which table we want to query the data.

## Understanding projections

Now, let's query only the name and age of each user:

```sql
SELECT name, age FROM users;
```

```json
{
    "name": "Gon",
    "age": 13,
}
{
    "name": "Kirua",
    "age": 14,
}
{
    "name": "Hisoka",
    "age": null
}
```

The result contains three documents, all of them have a `name` and `age` fields.

A projection guarantees that all the documents returned by the query will contain the selected fields, even if the original documents don't have that information. In our example, the `Hisoka` document doesn't have an `age` field, so its projected value is `null`.
The only exception is for the `*` wildcard, which projects all the fields of the original document.

## Querying nested fields

Let's determine who is the father of our users:

```sql
SELECT name, parent.father FROM users;
```

```json
{
    "name": "Gon",
    "parents.father": "Ging Freecs"
}
{
    "name": "Kirua",
    "parents.father": "Silva Zoldyck"
}
{
    "name": "Hisoka",
    "parents.father": null
}
```

In this example, we used the [dot notation]({{< relref "/docs/genji-sql/documents" >}}#dot-notation) to select the `parent.father` field of our users.

Let's add the information about the first ability they master:

```sql
SELECT name, parent.father, abilities.0 FROM users;
```

```json
{
    "name": "Gon",
    "parents.father": "Ging Freecs",
    "abilities.0": "Jajanken"
}
{
    "name": "Kirua",
    "parents.father": "Silva Zoldyck",
    "abilities.0": "Lighning Palm"
}
{
    "name": "Hisoka",
    "parents.father": null,
    "abilities.0": "Bungee Gum"
}
```

`abilities.0` is a dot notation that indicates to select the element at index `0` of the `abilities` array.

## Controlling the name of projected fields

The result of the query above contains fields named `parents.father` and `abilities.0`, which isn't that great. Let's rename them to more clean names:

```sql
SELECT name, parent.father AS father, abilities.0 AS main_skill FROM users;
```

```json
{
    "name": "Gon",
    "father": "Ging Freecs",
    "main_skill": "Jajanken"
}
{
    "name": "Kirua",
    "father": "Silva Zoldyck",
    "main_skill": "Lighning Palm"
}
{
    "name": "Hisoka",
    "father": null,
    "main_skill": "Bungee Gum"
}
```

The `AS` clause allows creating *aliases* to rename projected fields.

## Filter documents

Until now, we always performed our queries on every document of the table.
Let's only query those whose `nen` field is `Transmutation`.

```sql
SELECT name FROM users WHERE nen = 'Transmutation';
```

```json
{
    "name": "Kirua"
}
{
    "name": "Hisoka"
}
```

This time, the result contains only two documents.

The `WHERE` clause allows filtering the documents returned. To do that, it evaluates an [expression]({{< relref "/docs/genji-sql/expressions" >}}) on every document:

- if the result of the evaluation is *truthy*, the document is selected
- if the result of the evaluation is *falsy*, the document is filtered out

```sql
SELECT name, age FROM users WHERE age < 14;
```

```json
{
    "name": "Gon",
    "age": 13
}
```

In this example, only Gon satisfies the query:

- Kirua's age is greater 14 which is not `< 14`
- Hisoka's age is `null`, which is also not `< 14`

## Ordering results

The order in which results are returned can be controlled, using the `ORDER BY` clause

```sql
SELECT name, age FROM users ORDER BY age;
```

```json
{
    "name": "Hisoka",
    "age": null
}
{
    "name": "Gon",
    "age": 13
}
{
    "name": "Kirua",
    "age": 14
}
```

The order in which documents will appear depends on three factors:

- the *direction* or the order
- the *type* of the field used for ordering
- the *value* of the field used for ordering

By default, the direction is ascending, from the smallest value to the highest.

When it comes to ordering, there is a hierarchy between types:

`NULL` < `BOOLEAN` < numbers < `TEXT` or `BLOB`

In the example above, the `age` field of Hisoka doesn't exist, so it is treated as `null`, and then appears first in the result.

Then, Gon and Kirua have an `age` field which is an `INTEGER`, there are compared with each other and returned in ascending order.

The direction can be controlled by using `ASC` or `DESC` clauses.

```sql
SELECT name, age FROM users ORDER BY age ASC;
```

```json
// returns the same results as above
```

```sql
SELECT name, age FROM users ORDER BY age DESC;
```

```json
{
    "name": "Kirua",
    "age": 14
}
{
    "name": "Gon",
    "age": 13
}
{
    "name": "Hisoka",
    "age": null
}
```

## Limiting and skipping results

The `LIMIT` clause is executed after `WHERE` and `ORDER BY` and allows controlling the number of final results.

```sql
SELECT name FROM users WHERE nen = 'Transmutation' ORDER BY age DESC LIMIT 1;
```

```json
{
    "name": "Hisoka"
}
```

`LIMIT` must be followed by the number of maximum results. In this example, we limited the results to 1.

It is also possible to skip results, using the `OFFSET` clause. It is executed after the `WHERE` and `ORDER BY` clauses, but right before `LIMIT`.

```sql
SELECT name FROM users ORDER BY name LIMIT 2 OFFSET 1;
```

```json
{
    "name": "Hisoka"
}
{
    "name": "Kirua"
}
```

## Using functions

Projections can also use functions to add more power to the queries.

### Select the primary key

Every document has a primary key, which is a unique value identifying it.
When a document is inserted without an explicit primary key, an implicit one is created automatically. Implicit primary key don't appear in the results though, even when using `SELECT *`.
To select them, we can use the `pk()` function.

```sql
SELECT pk(), name FROM users;
```

```json
{
    "name": "Gon",
    "pk()": 1
}
{
    "name": "Kirua",
    "pk()": 2
}
{
    "name": "Hisoka",
    "pk()": 3
}
```
