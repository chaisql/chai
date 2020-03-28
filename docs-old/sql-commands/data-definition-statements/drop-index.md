---
description: Remove an index and all of its content
---

# DROP INDEX

## Synopsis

```sql
DROP INDEX [IF EXISTS] index_name
```

The `DROP INDEX`statement is used to remove an index and all of its content from the Genji database.

## Parameters

#### `IF EXISTS`

By default, if the index doesn't exist, Genji will return an error. If `IF EXISTS` is specified, no error will be returned.

#### `index_name` 

Name of the index.  
_Type_: [identifier](../../sql-syntax/lexical-structure.md#identifiers)

## Examples

Drop index

```sql
DROP TABLE teams_name
```

Drop index teams if it exists

```sql
DROP TABLE IF EXISTS teams_name
```

