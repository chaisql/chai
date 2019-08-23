# Genji

[![Build Status](https://travis-ci.org/asdine/genji.svg)](https://travis-ci.org/asdine/genji)
[![GoDoc](https://godoc.org/github.com/asdine/genji?status.svg)](https://godoc.org/github.com/asdine/genji)
[![Slack channel](https://img.shields.io/badge/slack-join%20chat-green.svg)](https://gophers.slack.com/messages/CKPCYQFE0)

Genji is a powerful embedded database build on top of key-value stores. It supports various engines that write data on-disk, like [BoltDB](https://github.com/etcd-io/bbolt) and [Badger](https://github.com/dgraph-io/badger), or in memory.

It provides a complete framework with multiple APIs that can be used to manipulate, manage, read and write data.

Genji tables are schemaless and can be manipulated using the table package, which is a low level functional API or by using the query package which is a powerful SQL like query engine.

## Features

* **Abstract storage**: Stores data on disk using [BoltDB](https://github.com/etcd-io/bbolt), [Badger](https://github.com/dgraph-io/badger) or in memory
* **No reflection**: Uses code generation to map Go structures to tables
* **Type safe APIs**: Generated code allows to avoid common errors and avoid reflection
* **SQL Like queries**: Genji provides a query engine to run complex queries
* **Index support**: Declare indexes and let Genji deal with them.
* **Low memory footprint**: Read thousands of records with O(1) memory thanks to the streaming system
* **Complete framework**: Use Genji to manipulate tables, extend the query system or implement you own engine.

## Installation

Install the Genji library and command line tool

``` bash
go get -u github.com/asdine/genji/...
```

## Usage

Declare a structure. Note that, even though struct tags are defined, Genji **doesn't use reflection**.

``` go
// user.go

type User struct {
    ID int64    `genji:"pk"`
    Name string `genji:"index"`
    Age int
}
```

Generate code to make that structure compatible with Genji.

``` bash
genji -f user.go -s User
```

This command generates a file that contains APIs specific to the `User` type.

``` go
// user.genji.go

// The User type gets new methods that implement some Genji interfaces.
func (u *User) Field(name string) (field.Field, error) {}
func (u *User) Iterate(fn func(field.Field) error) error {}
func (u *User) ScanRecord(rec record.Record) error {}
func (u *User) PrimaryKey() ([]byte, error) {}
func (u *User) Indexes() map[string]index.Options {}

// A UserFields type is generated to ease writing queries.
type UserFields struct {
    ID   query.Int64Field
    Name query.StringField
    Age  query.IntField
}
func NewUserFields() UserFields {}
```

### Example

``` go
package main

func main() {
    // Instantiate an engine, here we'll store everything in memory
    ng := memory.NewEngine()

    // Instantiate a DB using the engine
    db, err := genji.New(ng)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Genji provides two types of transactions:
    // - read-only, using the db.View or db.ViewTable methods
    // - read-write, using the db.Update or db.UpdateTable methods

    // Create a read-write transaction to initialize the table.
    // This ensures the table and all the indexes are created.
    err := db.Update(func(tx *genji.Tx) error {
        _, err := tx.InitTable("users", new(User))
        return err
    })
    if err != nil {
        log.Fatal(err)
    }

    // Create a read-write transaction to create one or more users.
    err = db.Update(func(tx *genji.Tx) error {
        t, err := tx.GetTable("users")
        if err != nil {
            return err
        }

        // Insert a user into the users table
        _, err := t.Insert(&User{
            ID:   10,
            Name: "foo",
            Age:  32,
        })
    })
    if err != nil {
        log.Fatal(err)
    }

    // Let's select a few users
    var users []User

    // This time we will create a read transaction to run the query
    err = db.View(func(tx *genji.Tx) error {
        t, err := tx.GetTable("users")
        if err != nil {
            return err
        }

        // Create a UserFields. This generated type contains information about the fields of the User record
        // that can be used to ease writing queries.
        f := NewUserFields()

        return query.
            // Use the table schema to select the fields of your choice
            Select().
            // The from method expects a type who can select the right table from the transaction,
            // the UserTableDescriptor implements the required interface.
            From(t).
            // The fields of f are generated based on the fields of the User structure and their type
            // Here, because Age is an int, the Gte method expects an int.
            Where(f.Age.Gte(18)).
            // Run the query using the transaction.
            // This function returns a Stream object that can be used to read records.
            Run(tx).
            // Iterate over the stream to initiates the query.
            // Each record is read one by one from the database
            // to ensure O(1) memory most of the tine.
            Iterate(func(recordID []byte, r record.Record) error {
                var u User
                err := u.ScanRecord(r)
                if err != nil {
                    return err
                }

                users = append(users, u)
                return nil
            })
    })
    if err != nil {
        log.Fatal(err)
    }
}
```

## Engines

Genji currently supports storing data in [BoltDB](https://github.com/etcd-io/bbolt), [Badger](https://github.com/dgraph-io/badger) and in-memory.

### Use the BoltDB engine

``` go
import (
    "log"

    "github.com/asdine/genji"
    "github.com/asdine/genji/engine/bolt"
)

func main() {
    // Create a bolt engine
    ng, err := bolt.NewEngine("genji.db", 0600, nil)
    if err != nil {
        log.Fatal(err)
    }

    // Pass it to genji
    db := genji.New(ng)
    defer db.Close()
}
```

### Use the Badger engine

``` go
import (
    "log"

    "github.com/asdine/genji"
    "github.com/asdine/genji/engine/badger"
    bdg "github.com/dgraph-io/badger"
)

func main() {
    // Create a badger engine
    ng, err := badger.NewEngine(bdg.DefaultOptions("genji")))
    if err != nil {
        log.Fatal(err)
    }

    // Pass it to genji
    db, err := genji.New(ng)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
}
```

### Use the memory engine

``` go
import (
    "log"

    "github.com/asdine/genji"
    "github.com/asdine/genji/engine/memory"
)

func main() {
    // Create a memory engine
    ng := memory.NewEngine()

    // Pass it to genji
    db, err := genji.New(ng)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
}
```

## Tags

Genji scans the struct tags at compile time, not at runtime, and it uses this information to generate code.

Here is a list of supported tags:

* `pk` : Indicates that this field is the primary key. The primary key can be of any type. If this tag is not provided, Genji uses its own internal autoincremented id
* `index` : Indicates that this field must be indexed.
* `index(unique)` : Indicates that this field must be indexed and that it must associate only one recordID per value.
