---
title: "CREATE INDEX"
date: 2020-03-29T17:54:36+04:00
weight: 6
description: >
  Define a new index
---

## Synopsis

```sql
CREATE [UNIQUE] INDEX [IF NOT EXISTS] index_name ON table_name (field_name)
```

The `CREATE INDEX`statement is used to create a new index for a Genji table. Every record of a table will be indexed, even if it doesn't contain the selected `field_name`, in which case, the value indexed will be `NULL`.

## Parameters

#### `IF NOT EXISTS`

By default, if an index with the same name already exists, Genji will return an error. If `IF NOT EXISTS` is specified, no error will be returned.

#### `index_name` 

Name of the index, must be unique.  
_Type_: [identifier](../../sql-syntax/lexical-structure.md#identifiers)

#### `table_name`

Name of the table that will be indexed. The table must be created prior of creating the new index.  
_Type_: [identifier](../../sql-syntax/lexical-structure.md#identifiers)

#### `field_name`

Name of the field that will be indexed. If the field is not present in the record, `NULL` will be used as value.  
_Type_: [identifier](../../sql-syntax/lexical-structure.md#identifiers)

#### `UNIQUE`

If specified, only one value will be associated to a given record key and an error will be returned if trying to insert another record with the same value.

The conversion follows the following rules:

## Examples

Create index on a team name

```sql
CREATE TABLE teams;
CREATE INDEX teams_name ON teams(name)
```

Create index if not exists

```sql
CREATE INDEX IF NOT EXISTS teams_name ON teams(name)
```

