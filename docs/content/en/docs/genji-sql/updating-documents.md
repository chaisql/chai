---
title: "Updating Documents"
date: 2020-04-05T11:37:36+04:00
weight: 60
description: >
    How to use the UPDATE statement to modify documents in a table
---

The `UPDATE` statement makes it possible to update one or more documents in a table.

Consider a table `users` with the following documents in it.

```json
{
    "name": "Koruto",
    "age": 1
}
{
    "name": "Leol",
    "group": "Chimera-Ant",
    "age": 2
}
```

```sql
UPDATE users SET group = "Chimera Ant"
```

Let's break it down:

- `UPDATE users` runs the `UPDATE` statement on the `users` table
- `SET` indicates the list of changes we want to perform
- `group = "Chimera Ant"` sets the `group` field of the document to the value "Chimera Ant"

Without a `WHERE` clause, this statement will run on all the documents of the table. Here is the state of the table after running this command:

```json
{
    "name": "Koruto",
    "group": "Chimera Ant"
}
{
    "name": "Leol",
    "group": "Chimera Ant"
}
```

The first document didn't have a `group` field before. It's because the `SET` clause actually sets fields in the document, regardless of their existence. This is a good way to add new fields to documents.

Since we can add or modify fields using the `SET` clause, it is also possible to delete fields using the `UNSET` clause:

```sql
UPDATE users UNSET age;
```

This will delete the `age` field from all the documents. If the field doesn't exist it does nothing.

To update only a subset of documents, we can use the `WHERE` clause. In the following example, only the documents that satisfy the `age = 2` condition will be updated.

```sql
UPDATE users SET group = "Chimera Ant" WHERE age = 2;
```
