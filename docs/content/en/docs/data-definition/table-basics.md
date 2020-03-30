---
title: "Table Basics"
date: 2020-03-30T20:27:04+04:00
description: >
    How to create and remove tables
---

Though Genji stores its data in tables, there is no concept of rows or columns. A Genji table is simply a collection of documents.

Each document is assigned to a primary key, which is a unique identifier.

The order in which documents are returned when reading a table is not guaranteed unless sorted explicitly.

Unlike relational databases, tables are schemaless, there is no need to specify a schema when creating one.
This means that, by default, documents stored in a table can be completely different from one another.
Optionally, it is possible to define constraints on a list of fields, to control their type, if they are required or not, if they can be null, etc. for every document of a table.

To create a table with no constraints, use the `CREATE TABLE` command.

```sql
CREATE TABLE teams;
```

This will create a table `teams` that can hold any document. An auto-incrementing primary key will be generated every time a document is inserted.

Creating a table with constraints uses a notation that is close to other relational databases.

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    age INT8
)
```

This will create a table `users` with the following constraints:

* All documents must have a non-empty `id` field, whose type can be converted to an integer. This field will be used as the primary key of the table and will be stored as an integer.
* All documents must have a non-empty `name` field that can be converted to `TEXT`.
* If a document has an `age` field, it will be converted to a one-byte integer.

Unlike relational databases though, a document doesn't have to contain only the fields described in the constraint list. A constraint only applies to its associated field.

`CREATE TABLE` will return an error if the table already exists.

To remove a table and all of its content, use the `DROP TABLE` command:

```sql
DROP TABLE users
```

This will remove the `users` table and all of its documents. If `DROP TABLE` is called on a non-existing table, it will return an error.
