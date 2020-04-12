---
title: "Using Indexes"
date: 2020-04-12T09:39:36+04:00
weight: 80
description: >
    How to manipulate indexes
---

Under certain conditions, indexes can be used to speed-up queries.

Indexes are created using the `CREATE INDEX` statement.

```sql
CREATE INDEX idx_nen ON users(nen);
CREATE INDEX idx_address_city ON users(address.city);
CREATE INDEX `main skill index` ON users(skills.0);
```

Every index must have a name and must indicate on which table and field they operate. Note that is it possible to index nested fields or array values as well.

Once an index is created, every document inserted *afterward* will be indexed. Creating an index doesn't reindex existing documents.

To index older documents, use the `REINDEX` statement.

```sql
/* Reindex all the indexes of all tables */
REINDEX;
/* Reindex all the indexes of a given table */
REINDEX users;
/* Reindex a given index, if a table with the same name doesn't exist */
REINDEX idx_nen;
```

To make sure all documents of a table have a unique value for a given field, use the `CREATE INDEX` statement:

```sql
CREATE UNIQUE INDEX idx_email ON users(email);
```

A unique index ensures that the indexed fields do not store duplicate values.
Note that `NULL` values will have the same constraints, meaning that only one document who doesn't contain the indexed field, or whose field is equal to `NULL` will be able to be inserted.

To delete indexes, use the `DELETE INDEX` statement

```sql
DELETE INDEX idx_address_city;
```
