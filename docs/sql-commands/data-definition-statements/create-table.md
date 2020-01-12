---
description: Define a new table
---

# CREATE TABLE

## Synopsis

```sql
CREATE TABLE [IF NOT EXISTS] table_name [(field_constraint)]

field_constraint:
    (field_path field_type [PRIMARY KEY])+ [, field_constraint ]
```

The `CREATE TABLE` statement is used to create a new table in the Genji database. Tables being schema-less, there is no need to specify a schema during the creation of the table. Instead, Genji provides a way to enforce the type of certain fields, rather than specifying a complete schema that all documents must abide to.

## Parameters

#### `table_name`

Name of the table.  
_Type_: [identifier](../../sql-syntax/lexical-structure.md#identifiers)

#### `IF NOT EXISTS`

By default, if a table with the same name already exists, Genji will return an error. If `IF NOT EXISTS` is specified, no error will be returned.

#### `field_constraint`

If specified, it will be used to ensure certain fields have the right types before being stored in the table, automatically converting them when it's possible.

#### `field_path`

Path of the field, in dot notation.  
_Type_: [identifier](../../sql-syntax/lexical-structure.md#identifiers)

#### `field_type`

The type of the field. If the field of a document doesn't match exactly this type but if its type is compatible, it will be automatically converted.  
_Type_: [data type](../../sql-syntax/data-types.md)

The conversion follows the rules defined in [this page](../../sql-syntax/data-types.md#conversion).

#### `PRIMARY KEY`

If specified, the field will be used as the primary key of the table. There can only be one primary key per table. If no primary key is specified, an internal auto-incremented key will be used as primary key.

## Examples

Create table teams

```sql
CREATE TABLE teams
```

Create table teams if not exists

```sql
CREATE TABLE IF NOT EXISTS teams
```

Create table teams with primary key

```sql
CREATE TABLE teams (id INTEGER PRIMARY KEY)
```

Create table teams with type constraints on a few fields

```sql
CREATE TABLE teams (city STRING, players_count INTEGER)
```

Create table teams with primary key and type constraints

```sql
CREATE TABLE teams (id INTEGER PRIMARY KEY, name STRING)
```
