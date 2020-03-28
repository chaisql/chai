---
description: Remove a table and all of its content
---

# DROP TABLE

## Synopsis

```sql
DROP TABLE [IF EXISTS] table_name
```

The `DROP TABLE`statement is used to remove a table and all of its content from the Genji database.

## Parameters

#### `IF EXISTS`

By default, if the table doesn't exist, Genji will return an error. If `IF EXISTS` is specified, no error will be returned.

#### `table_name` 

Name of the table.  
_Type_: [identifier](../../sql-syntax/lexical-structure.md#identifiers)

## Examples

Drop table teams

```sql
DROP TABLE teams
```

Drop table teams if exists

```sql
DROP TABLE IF EXISTS teams
```

