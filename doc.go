/*
Package genji implements a SQL database on top of key-value stores.
Genji supports various engines that write data on-disk, like BoltDB or Badger, and in memory.

Genji tables are schemaless and can be mapped to Go structures without reflection: Genji relies on code generation to translate data to and from Go structures.

Engine and key value stores

Genji relies on key-value stores to store data. The engine package defines interfaces that must be implemented
to be used by the Genji database. As long as an implementation satisfies the engine package requirements, data can be stored anywhere.

Out of the box, Genji provides three implementations: BoltDB, Badger and in-memory.

See the engine package documentation for more details.

Field, Record, Table and Stream

Genji defines its own semantic to describe data.
Data stored in Genji being schemaless, the usual SQL triplet "column", "row", "table" wasn't chosen
for the vocabulary of this library. Also, Genji is a database written in Go for Go, and its primary goal
is to map structures and maps to tables, though it's not limited to that.

That's why the triplet "field", "record" and "table" was chosen.

A field is a piece of information that has a type, content, and a name.
The field is equivalent to the SQL column, though it might contain nested fields in the future.

A record is a group of fields. It is an interface that can be implemented manually or by using Genji's code generation.
This is equivalent to the SQL row. It is managed by the record package, which also provides ways to encode and decode records.

A table is an abstraction on top of K-V stores that can read and write records.
This is equivalent to the SQL table.

A stream can read data from a table, record by record, and apply transformations, filter them, etc.
See the record package for more information.

These are the basic building blocks of the Genji database.

SQL support

Queries can be executed in two ways:

- Using Genji's streaming API

- Using Genji as a driver for the database/sql package

See code examples below to learn how to use both APIs.

The CREATE TABLE statement

Genji tables are schemaless, that means that there's no need to specify a schema when creating a table.
Creating a table is as simple as:

  CREATE TABLE tableName

or:

  CREATE TABLE IF NOT EXISTS tableName

The CREATE INDEX statement

Only one-field indexes are currently supported:

  CREATE INDEX indexName ON tableName (fieldName)

with a unique constraint:

  CREATE UNIQUE INDEX indexName ON tableName (fieldName)

with if not exists:

  CREATE UNIQUE INDEX IF NOT EXISTS indexName ON tableName (fieldName)

The DROP TABLE statement

This will return an error if the table doesn't exists.

  DROP TABLE tableName

This won't:

  DROP TABLE IF EXISTS tableName

The DROP INDEX statement

This will return an error if the index doesn't exists.

  DROP INDEX indexName

This won't:

  DROP INDEX IF EXISTS indexName

The INSERT statement

Since tables are schemaless, providing a list of field names is mandatory when using the VALUES clause.

  INSERT INTO tableName (fieldNameA, fieldNameB, fieldNameC) VALUES (10, true, "bar"), ("baz", 3.14, -10)

Inserting records is also supported with the VALUES clause.
Genji SQL represents records as a set of key value pairs.
Note that field names are forbidden when using the VALUES clause.

  INSERT INTO tableName VALUES (fieldNameA: 10, fieldNameB: true, fieldNameC: "bar"), (fieldNameA: "bab", fieldNameD: 3.14)

The SELECT statement

Explicit field names:

  SELECT fieldNameA, fieldNameB FROM tableName

Using the wildcard:

  SELECT * FROM tableName

With the WHERE clause. See below for documentation about expressions.

  SELECT * FROM tableName WHERE <expression>

With LIMIT and OFFSET:

  SELECT * FROM tableName LIMIT 10
  SELECT * FROM tableName OFFSET 20

When both LIMIT and OFFSET are used, LIMIT must appear before OFFSET:

  SELECT * FROM tableName LIMIT 10 OFFSET 20

The DELETE statement

  DELETE FROM tableName
  DELETE FROM tableName WHERE <expression>

The UPDATE statement

  UPDATE tableName SET fieldNameA = <expression>, fieldNameB = <expression>
  UPDATE tableName SET fieldNameA = <expression>, fieldNameB = <expression> WHERE <expression>

Expressions

Litteral values:

  10    Integers, interpreted as int64
  3.14  Decimals, interpreted as float64
  true  Booleans, interpreted as bool
  'foo' Strings, interpreted as string

Identifiers:

  foo   Any string without quotes is interpreted as a field name
  "foo" Any double quoted string is interpreted as a field name

Binary operators: Comparison operators

During comparison, only the values of numbers are compared, not the types,
which allows comparing signed integers with unsigned integers or floats for example.

When evaluating a binary expression, the left and right expressions are evaluated first
then compared.

 <exprA> = <exprB>  Evaluates to true if two expressions are equal
 <exprA> != <exprB>  Evaluates to true if two expressions are not equal
 <exprA> > <exprB>  Evaluates to true if exprA is greater than exprB
 <exprA> >= <exprB> Evaluates to true if exprA is greater than or equal to exprB
 <exprA> < <exprB>  Evaluates to true if exprA is lesser than exprB
 <exprA> <= <exprB> Evaluates to true if exprA is lesser than or equal to exprB


Binary operators: Logical operators

 <exprA> AND <exprB>   Evaluates to true if exprA and exprB evaluate to true
 <exprA> OR <exprB>    Evaluates to true if exprA or exprB evaluate to true

Parameters

Genji SQL supports two kind of parameters: Positional parameters and named parameters

Positional parameters are specified using the '?' character:

  db.Query("SELECT * FROM tableName WHERE foo > ? AND bar = ?", 10, "baz")

Named parameters are specified using the '$' character. Note that passing a named parameter requires using the
database/sql package from the standard library.

  db.Query("SELECT * FROM tableName WHERE foo > $a AND bar = $b", sql.Named("a", 10), sql.Named("b", "baz"))

Note that combining both named and positional parameters is forbidden.

Struct support and code generation

Genji also supports structures for reading and writing records, but because Genji doesn't use reflection, these structures must implement a couple of interface
to be able to interat with Genji properly.
In order to simplify these implementation, Genji provides a command line code generator that can be used with the go:generate command.

Let's assume that there is a file named user.go containing the following type:

  type User struct {
	  ID   int64
	  Name string
	  Age  uint32
  }

The genji command line can be used as follows to generate the code:

  genji -f user.go -s User

This will generate a file named user.genji.go containing the following methods

  // The User type gets new methods that implement some Genji interfaces.
  func (u *User) GetByField(name string) (document.Field, error) {}
  func (u *User) Iterate(fn func(document.Field) error) error {}
  func (u *User) ScanDocument(rec document.Document) error {}
  func (u *User) Scan(src interface{}) error {}

The User type now implements all the interfaces needed to interact correctly with the database APIs.
See the examples in this page to see how it can be used.
*/
package genji
