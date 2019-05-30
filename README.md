# Genji

[![Build Status](https://travis-ci.org/asdine/genji.svg)](https://travis-ci.org/asdine/genji)
[![GoDoc](https://godoc.org/github.com/asdine/genji?status.svg)](https://godoc.org/github.com/asdine/genji)
[![Go Report Card](https://goreportcard.com/badge/github.com/asdine/genji)](https://goreportcard.com/report/github.com/asdine/genji)

Genji is a powerful embedded relational database build on top of key-value stores. It supports various engines that write data on-disk, like [BoltDB](https://github.com/etcd-io/bbolt), or in memory.

It provides a complete framework with multiple APIs that can be used to manipulate, manage, read and write data.

Genji supports schemaful and schemaless tables that can be manipulated using the table package, which is a low level functional API
or by using the query package which is a powerful SQL like query engine.

## Features

- **Abstract storage**: Stores data on disk using [BoltDB](https://github.com/etcd-io/bbolt) or in memory (_[Badger](https://github.com/dgraph-io/badger) is coming soon_)
- **No reflection**: Uses code generation to map Go structure to tables
- **Flexible structure**: Declare schemaful or schemaless tables
- **Type safe APIs**: Generated code allows to avoid common errors
- **SQL Like queries**: Genji provides a query engine to run complex queries
- **Index support**: Declare indexes and let Genji deal with them.
- **Complete framework**: Use Genji to manipulate tables, extend the query system or implement you own engine.

## Installation

Install the Genji library and command line tool

```bash
go get -u github.com/asdine/genji/...
```

## Usage

Declare a structure. Note that, even though struct tags are defined, Genji doesn't use reflection.

```go
// user.go

type User struct {
    ID int64 `genji:"pk"`
    Name string `genji:"index"`
    Age int
}
```

Generate a schemaless table

```bash
genji -f user.go -s User
```

or a schemaful table

```bash
genji -f user.go -S User
```

This command generates a file that contains APIs specific to the `User` type.

```go
// User gets new methods that implement some Genji interfaces.
func (u *User) Field(name string) (field.Field, error) {}
func (u *User) Iterate(fn func(field.Field) error) error {}
func (u *User) ScanRecord(rec record.Record) error {}
func (u *User) Pk() ([]byte, error) {}

// A UserStore type is generated to simplify interaction with the User table
// and simplify common operations.
type UserStore struct {
    *genji.Store
}
func NewUserStore(db *genji.DB) *UserStore {}
func NewUserStoreWithTx(tx *genji.Tx) *UserStore {}
func (u *UserStore) Insert(record *User) (err error) {}
func (u *UserStore) Get(pk int64) (*User, error) {}
func (u *UserStore) Delete(pk int64) error {}
func (u *UserStore) List(offset, limit int) ([]User, error) {}
func (u *UserStore) Replace(pk int64, record *User) error {}

// A UserQuerySelector is generated to ease writing queries.
type UserQuerySelector struct {
    ID   query.Int64Field
    Name query.StringField
    Age  query.IntField
}
func NewUserQuerySelector() UserQuerySelector {}
func (*UserQuerySelector) Table() query.TableSelector {}
func (s *UserQuerySelector) All() []query.FieldSelector {}

// UserResult can receive the result of a query that returns users.
type UserResult []User
func (u *UserResult) ScanTable(tr table.Reader) error {}
```

### Example

```go
package main

func main() {
    // Instantiate an engine
    ng := memory.NewEngine()

    // Instantiate a DB using the engine
    db, err := genji.New(ng)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Create a UserStore from the generated code
    // A Store is a high level type safe API that wraps a Genji table.
    users := NewUserStore(db)

    // Insert a user using the generated methods
    err = users.Insert(&User{
        ID:   10,
        Name: "foo",
        Age:  32,
    })
    if err != nil {
        log.Fatal(err)
    }

    // Get a user using its primary key
    u, err := users.Get(10)
    if err != nil {
        log.Fatal(err)
    }

    // List users
    list, err := users.List(0, 10)
    if err != nil {
        log.Fatal(err)
    }

    // Run complex queries
    qs := NewUserQuerySelector()
    var result UserResult
    err = users.View(func(tx *genji.Tx) error {
        // SELECT ID, Name FROM User where Age >= 18
        return query.Select(qs.ID, qs.Name).From(qs.Table()).Where(qs.Age.Gte(18)).
            Run(tx).
            Scan(&result)
    })
    if err != nil {
        log.Fatal(err)
    }
}
```

## Engines

Genji currently supports storing data in [BoltDB](https://github.com/etcd-io/bbolt) and in-memory (_[Badger](https://github.com/dgraph-io/badger) is coming soon_)

### Use the BoltDB engine

```go
import (
    "log"

    "github.com/asdine/genji"
    "github.com/asdine/genji/engine/bolt"
)

// Create a bolt engine
ng, err := bolt.NewEngine("genji.db", 0600, nil)
if err != nil {
    log.Fatal(err)
}

// Pass it to genji
db, err := genji.New(ng)
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```

### Use the memory engine

```go
import (
    "log"

    "github.com/asdine/genji"
    "github.com/asdine/genji/engine/memory"
)

// Create a memory engine
ng, err := memory.NewEngine()
if err != nil {
    log.Fatal(err)
}

// Pass it to genji
db, err := genji.New(ng)
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```
