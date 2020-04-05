---
title: "Inserting Documents"
date: 2020-04-05T08:02:26+04:00
weight: 50
description: >
    How to use the INSERT statement to insert documents in a table
---

When a table is created, it contains no documents. The `INSERT` statement can be used to add one or more new documents to the table.

## Inserting documents in tables with no field constraints

Consider a table created with the following statement:

```sql
CREATE TABLE users;
```

This table doesn't have any constraint and thus can contain any kind of documents.

Let's insert a document:

```sql
INSERT INTO users (name, age) VALUES ("Gon", 13);
```

Let's break it down:

- `INSERT INTO users`: tells Genji to run the statement on the `users` table
- `(name, age)`: lists the fields of the document we wish to create
- `VALUES ("Gon", 13)`: list the respective values of these fields in order

Here is the JSON representation of the document created by this statement:

```json
{
    "name": "Gon",
    "age": 13
}
```

It is possible to create multiple documents in the same statement:

```sql
INSERT INTO users (name, age) VALUES ("Gon", 13), ("Kirua", 14);
```

This will create two documents in the `users` table:

```json
{
    "name": "Gon",
    "age": 13
}
{
    "name": "Kirua",
    "age": 14
}
```

Until now, we created documents with the same shape, but nothing prevents us from inserting documents with different fields:

```sql
INSERT INTO users (name, address) VALUES ("Kurapika", {city: "York Shin City", "region": "Yorubian"});
INSERT INTO users (first_name, `last name`, skills) VALUES ("Zeno", 'Zoldik', ["Dragon Dive", "Dragon Head"] );
```

It is also possible to omit the list of fields and use the [document expression]({{< relref "/docs/genji-sql/lexical-structure" >}}#documents):

```sql
INSERT INTO users VALUES {name: "Hisoka", "age": "unknown"}
```

Note that in this example, the `age` field type is `TEXT`. It's because field types don't have to match those of the documents created previously, documents are independent and self-contained.

## Inserting documents in tables with field constraints

Now, let's consider having the following table:

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    age INT8
)
```

Only documents satisfying the following field constraints can be inserted in the `users` table:

- the document must have a non-null `id` field. It must be convertible to an `INTEGER`. Since this field will be used as the primary key of the table, each `id` must be unique.
- the document must have a non-null `name` field. It must be convertible to a `TEXT`.
- the document may have an `age` field. If it exists, it must be convertible to an `INT8`.
- the document may have any other fields.

The conversion compatible table can be found in the [data types]({{< relref "/docs/genji-sql/data-types" >}}#explicit-conversion) page.

To see how it works, let's try inserting invalid documents:

```sql
/* totally different fields */
INSERT INTO users (first_name, alias) VALUES ('Genthru', 'The Bomber');
```

Error:

> field "id" is required and must be not null

Let's add an `id`:

```sql
INSERT INTO users (id, first_name, alias) VALUES ("some id", 'Genthru', 'The Bomber');
```

Error:

> can't convert "text" to int64

We are trying to insert in `id` field of type `TEXT` into an `INTEGER` field, they are not [compatible]({{< relref "/docs/genji-sql/data-types" >}}#explicit-conversion).

Let's try with a `FLOAT64` this time:

```sql
INSERT INTO users (id, first_name, alias) VALUES (3.14, 'Genthru', 'The Bomber');
```

Error:

> cannot convert float64 value to integer without loss of precision

It is because of the `.14` part of the number, converting it to an integer means losing precision.

```sql
INSERT INTO users (id, first_name, alias) VALUES (1, 'Genthru', 'The Bomber');
```

Error:

> field "name" is required and must be not null

This time, it complains about the `name` field which is absent. Let's change that:

```sql
INSERT INTO users (id, name, alias) VALUES (1, 'Genthru', 'The Bomber');
```

It works!

Since `age` is doesn't have a `NOT NULL` clause, it didn't complain.

Also, the document contains an `alias` field and Genji didn't complain. Field constraints only apply on the field they are associated with, they don't care about the other ones. That's what makes Genji different from "schemaful" databases, where the schema describes exactly the number of columns a row must always have.

Let's add another one with an age field:

```sql
INSERT INTO users (id, name, age) VALUES (1, 'Biscuit', 57);
```

Error:

> duplicate document

This time we used the same `id` as before. Since `1` is already used by Genthru, let's pick another one:

```sql
INSERT INTO users (id, name, age) VALUES (1, 'Biscuit', 57);
```

It works!
