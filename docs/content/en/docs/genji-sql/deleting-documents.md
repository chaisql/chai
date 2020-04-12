---
title: "Deleting Documents"
date: 2020-04-12T09:29:10+04:00
weight: 70
description: >
    How to use the DELETE statement to delete documents from a table
---

Documents can be deleted using the `DELETE` statement.

Let's start with the simplest form:

```sql
DELETE FROM users;
```

This command deletes all the documents of the `users` table.

To delete only a few documents, use the `WHERE` clause:

```sql
DELETE FROM users WHERE age > 13;
```

For every document, the `WHERE` clause evaluates any [expression]({{< relref "/docs/genji-sql/expressions" >}}) that follows, here `age > 13`. If the result is truthy, the document gets deleted.

The `DELETE` statement doesn't return an error if no document matches the `WHERE` clause, or if there aren't any document in the table.
