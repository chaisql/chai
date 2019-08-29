/*
Package genji implements a database on top of key-value stores.
Genji supports various engines that write data on-disk, like BoltDB or Badger, and in memory.

It provides a complete framework with multiple APIs that can be used to manipulate, manage, read and write data.

Genji tables are schemaless and can be manipulated using the table package, which is a low-level streaming API
or by using the query package which is a powerful SQL like query engine.

Tables can be mapped to Go structures without reflection: Genji relies on code generation to translate data to and from Go structures.

The Genji Framework

Genji is designed as a framework. Each package can be accessed independently and most of the time they have a single purpose.
Here is how the most important packages are organized, from bottom to up:

                    +----------+            +----------+
      +------------->  engine  |      +---->+  field   |
      |             +-----^----+      |     +----^-----+
      |                   |           |          |
      |                   |           |          |
      |                   |           |          |
      |                   |           |     +----+-----+
      |                   |           +---->+  record  |
      |                   |           |     +----^-----+
      |                   |           |          |
      |                   |           |          |
      |                   |           |          |
  +---+----+        +-----+----+      |     +----+-----+
  | index  +<-------+  genji   +------+---->+  table   |
  +---^----+        +-----^----+      |     +----------+
      |                   |           |
      |                   |           |
      |             +-----+----+      |
      +-------------+  query   +------+
                    +----------+

Engine and key value stores

Genji relies on key-value stores to store data. The engine package defines interfaces that must be implemented
to be used by the Genji database. As long as an implementation satisfies the engine package requirements, data can be stored anywhere.

Out of the box, Genji provides three implementations: BoltDB, Badger and in-memory.

See the engine package documentation for more details.

Field, Record, and Table

Genji defines its own semantic to describe data.
Data stored in Genji being schemaless, the usual SQL triplet "column", "row", "table" wasn't chosen
for the vocabulary of this library. Also, Genji is a database written in Go for Go, and its primary goal
is to map structures and maps to tables, though it's not limited to that.

That's why the triplet "field", "record" and "table" was chosen.

A field is a piece of information that has a type, content, and a name. It is managed by the field package, which provides helpers
to create and manipulate them. The field is equivalent to the SQL column, though it might contain nested fields in the future.

  type Field struct {
	  Name string
	  Type Type
	  Data []byte
  }

A record is a group of fields. It is an interface that can be implemented manually or by using Genji's code generation.
This is equivalent to the SQL row. It is managed by the record package, which also provides ways to encode and decode records.

  type Record interface {
	  // Iterate goes through all the fields of the record and calls the given function by passing each one of them.
	  // If the given function returns an error, the iteration stops.
	  Iterate(fn func(field.Field) error) error
	  // GetField returns a field by name.
	  GetField(name string) (field.Field, error)
  }

A table is a group of records. It allows to read and write records. It is also an interface, but Genji provides implementations that should cover most of the use cases
and that use the engine key-value stores to fetch and store data.
This is equivalent to the SQL table. It is managed by the table package, which also provides a way to stream records from a table.

These are the basic building blocks of the Genji database. The other packages use them to build complex features such as SQL-Like queries,
database migrations, indexing, code generation, etc.

The genji package

The genji package is central and acts as the main entry point for using the database.
It leverages the features of most of the other packages, implementing some interfaces here,
importing some types there. Its table implementation takes advantage of the index package to provide automatic support
for indexing.

The query package then uses almost every other packages, including genji, to provide SQL Like queries.

Types, code generation and the absence of reflection

Genji's framework is self-sufficient and covers most of the use cases, but since it doesn't use reflection,
users are expected to implement the record interface to allow their types to be used with the API.
This is a design choice to make Genji APIs safe and use compile-time checks rather than runtime ones.

Instead, it is possible to use the genji command line to generate code. This tool will add methods to the structure of your choice
to implement the record interface.

Let's assume that there is a file named user.go containing the following type:

  type User struct {
	  ID   int64  `genji:"pk"`
	  Name string `genji:"index"`
	  Age  uint32
  }

Note that even if struct tags are defined, Genji won't use reflection. They will be parsed by the genji command-line tool
to generate code based on them. See the generator package for more information on the semantics of the struct tags.

The genji command line can be used as follows to generate the code:

  genji -f user.go -s User

This will generate a file named user.genji.go containing the following types and methods

  // The User type gets new methods that implement some Genji interfaces.
  func (u *User) GetField(name string) (field.Field, error) {}
  func (u *User) Iterate(fn func(field.Field) error) error {}
  func (u *User) ScanRecord(rec record.Record) error {}
  func (u *User) PrimaryKey() ([]byte, error) {}
  func (u *User) Indexes() map[string]index.Options

  // This type is used to simplify using the query package.
  type UserFields struct {
    ID   query.Int64Field
    Name query.StringField
    Age  query.IntField
  }
  func NewUserFields() UserFields {}

The User type now implements all the interfaces needed to interact correctly with the database APIs.
See the examples in this page to see how it can be used.
*/
package genji
