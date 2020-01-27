# SELECT

## Synopsis

```sql
SELECT selectors [from_clause] [where_clause] [limit_clause] [offset_clause]

selectors:
    (field_name | pk() | wildcard)+ [, selectors]

where_clause:
    WHERE expression

limit_clause:
    LIMIT integer

offset_clause:
    OFFSET integer
```

The `SELECT`statement is used to query data from a table. Each record returned by the query will contain the fields selected by the `selectors` expression. If a record stored in the table doesn't contain a selected field, the field won't be present in the associated result.

## Parameters

#### `field_name`

Name of a field to select for each matching record. If the record doesn't contain the selected field, it won't be present in the associated result.  
_Type_: [identifier](../../sql-syntax/lexical-structure.md#identifiers)

#### `pk()`

Special function that returns the primary key of the matching record. If no primary key has been specified during the creation of the table, it will return the default generated key. The key will be accessible under the `pk()` field of the result.

#### `wildcard`

Written `*`, selects all the fields present in the matching record.

#### `WHERE expression`

The optional `WHERE` clause allows filtering records returned by the query by using an expression. For each record, that expression will be evaluated:

- If the result is truthy, the record matches and will be returned by the `SELECT`query.
- If the result is falsy, the record doesn't match and is not returned.

#### `limit_clause`

The optional `LIMIT` clause will limit the number of returned records. The argument of limit must always be an [integer](../../sql-syntax/lexical-structure.md#integers).  
_Type_: [integer](../../sql-syntax/lexical-structure.md#integers)

#### `offset_clause`

The optional `OFFSET` clause will skip a certain number of matching records. The argument of offset must always be an [integer](../../sql-syntax/lexical-structure.md#integers).  
_Type_: [integer](../../sql-syntax/lexical-structure.md#integers)

## Examples

Select all fields of every records of the table

```sql
SELECT * FROM teams
```

Select only a few fields of every records of the table

```sql
SELECT name, city FROM teams
```

Select the primary key of every records of the table

```sql
SELECT pk() FROM teams
```

Mixing all kind of selectors

```sql
SELECT *, name, *, pk(), name FROM teams
```

Filtering records using the `WHERE` clause

```sql
SELECT * FROM teams WHERE city = 'Lyon'
```

Limiting and skipping

```sql
SELECT * FROM teams LIMIT 10
SELECT * FROM teams WHERE city = 'Lyon' LIMIT 10
SELECT * FROM teams OFFSET 5
SELECT * FROM teams WHERE city = 'Lyon' OFFSET 5
SELECT * FROM teams LIMIT 10 OFFSET 5
SELECT * FROM teams WHERE city = 'Lyon' LIMIT 10 OFFSET 5
```
