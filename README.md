# Genji

[![Build Status](https://travis-ci.org/asdine/genji.svg)](https://travis-ci.org/asdine/genji)
[![GoDoc](https://godoc.org/github.com/asdine/genji?status.svg)](https://godoc.org/github.com/asdine/genji)
[![Slack channel](https://img.shields.io/badge/slack-join%20chat-green.svg)](https://gophers.slack.com/messages/CKPCYQFE0)

Genji is an embedded SQL database build on top of key-value stores. It supports various engines that write data on-disk, like [BoltDB](https://github.com/etcd-io/bbolt) and [Badger](https://github.com/dgraph-io/badger), or in memory.

Genji tables are schemaless and can be manipulated using SQL queries. Genji is also compatible with the `database/sql` package.

## Installation

Install the Genji database

```bash
go get github.com/asdine/genji
```

## Usage

There are two ways of using Genji, either by using Genji's API or by using the [`database/sql`](https://golang.org/pkg/database/sql/) package.

### Using Genji's API

```go
// Create a database instance, here we'll store everything on-disk using the BoltDB engine
db, err := genji.Open("my.db")
if err != nil {
    log.Fatal(err)
}
// Don't forget to close the database when you're done
defer db.Close()

// Create a table. Genji tables are schemaless, you don't need to specify a schema.
err = db.Exec("CREATE TABLE user")

// Create an index.
err = db.Exec("CREATE INDEX idx_user_name ON test (name)")

// Insert some data
err = db.Exec("INSERT INTO user (id, name, age) VALUES (?, ?, ?)", 10, "Foo1", 15)
err = db.Exec("INSERT INTO user (id, name, age) VALUES (?, ?, ?)", 11, "Foo2", 20)

// Use a transaction
tx, err := db.Begin(true)
defer tx.Rollback()
err = tx.Exec("INSERT INTO user (id, name, age) VALUES (?, ?, ?)", 12, "Foo3", 25)
...
err = tx.Commit()

// Query some records
res, err := db.Query("SELECT * FROM user WHERE age > ?", 18)
// always close the result when you're done with it
defer res.Close()

// Iterate over the results
err = res.Iterate(func(d document.Document) error {
    var id int
    var name string
    var age int32

    err = document.Scan(d, &id, &name, &age)
    if err != nil {
        return err
    }

    fmt.Println(id, name, age)
    return nil
})

// Count results
count, err := res.Count()

// Get first record from the results
r, err := res.First()
var id int
var name string
var age int32
err = document.Scan(r, &id, &name, &age)

// Apply some transformations
err = res.
    // Filter all even ids
    Filter(func(d document.Document) (bool, error) {
        f, err := r.GetByField("id")
        ...
        id, err := f.DecodeToInt()
        ...
        return id % 2 == 0, nil
    }).
    // Enrich the records with a new field
    Map(func(d document.Document) (document.Document, error) {
        var fb document.FieldBuffer

        err := fb.ScanDocument(r)
        ...
        fb.Add(document.NewStringValue("Group", "admin"))
        return &fb, nil
    }).
    // Iterate on them
    Iterate(func(d document.Document) error {
        ...
    })
```

### Using database/sql

```go
// import Genji as a blank import
import _ "github.com/asdine/genji"

// Create a sql/database DB instance
db, err := sql.Open("genji", "my.db")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Then use db as usual
res, err := db.ExecContext(...)
res, err := db.Query(...)
res, err := db.QueryRow(...)
```

## Code generation

Genji also supports structs as long as they implement the `document.Document` interface for writes and the `document.Scanner` interface for reads.
To simplify implementing these interfaces, Genji provides a command line tool that can generate methods for you.

First, install the Genji command line tool:

```bash
go get github.com/asdine/genji/cmd/genji
```

Declare a structure. Note that, even though struct tags are defined, Genji **doesn't use reflection** for these structures.

```go
// user.go

type User struct {
    ID int64
    Name string
    Age int `genji:"age-of-the-user"`
}
```

Generate code to make that structure compatible with Genji.

```bash
genji gen -f user.go -s User
```

This command generates a file that adds methods to the `User` type.

```go
// user.genji.go

// The User type gets new methods that implement some Genji interfaces.
func (u *User) GetByField(name string) (document.Field, error) {}
func (u *User) Iterate(fn func(document.Field) error) error {}
func (u *User) ScanDocument(rec document.Document) error {}
func (u *User) Scan(src interface{}) error
```

Also, it will create mapping between struct fields and their corresponding `document.Field`. For that, it will apply the `strings.ToLower` function on the struct field name, unless the `genji` tag was specified for that field. If so, it will use the name found in the tag.

### Example

```go
// Let's create a user
u1 := User{
    ID: 20,
    Name: "foo",
    Age: 40,
}

// Let's create a few other ones
u2 := u1
u2.ID = 21
u3 := u1
u3.ID = 22

// It is possible to let Genji deal with analyzing the structure
// when inserting a record, using the VALUES clause
err := db.Exec(`INSERT INTO user VALUES ?, ?, ?`, &u1, &u2, &u3)
// Note that it is also possible to write records by hand
err := db.Exec(`INSERT INTO user VALUES ?, {userid: 21, name: 'foo', "age-of-the-user": 40}, ?`, &u1, &u3)

// Let's select a few users
var users []User

res, err := db.Query("SELECT * FROM user")
defer res.Close()

err = res.Iterate(func(d document.Document) error {
    var u User
    // Use the generated ScanDocument method this time
    err := u.ScanDocument(r)
    if err != nil {
        return err
    }

    users = append(users, u)
    return nil
})
```

## Engines

Genji currently supports storing data in [BoltDB](https://github.com/etcd-io/bbolt), [Badger](https://github.com/dgraph-io/badger) and in-memory.

### Use the BoltDB engine

```go
import (
    "log"

    "github.com/asdine/genji"
)

func main() {
    db, err := genji.Open("my.db")
    defer db.Close()
}
```

### Use the memory engine

```go
import (
    "log"

    "github.com/asdine/genji"
)

func main() {
    db, err := genji.Open(":memory:")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
}
```

### Use the Badger engine

The Badger engine must be installed first

```sh
go get github.com/asdine/genji/engine/badgerengine
```

Then, it can be instantiated using the `genji.New` function:

```go
import (
    "log"

    "github.com/asdine/genji"
    "github.com/asdine/genji/engine/badgerengine"
    "github.com/dgraph-io/badger"
)

func main() {
    // Create a badger engine
    ng, err := badgerengine.NewEngine(badger.DefaultOptions("mydb")))
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

## Genji shell

Besides generating code, the genji command line provides an SQL shell that can be used to create, modify and consult Genji databases.

Make sure the Genji command line is installed:

```bash
go get github.com/asdine/genji/cmd/genji
```

Example:

```bash
# Opening an in-memory database:
genji

# Opening a BoltDB database:
genji my.db

# Opening a Badger database:
genji --badger pathToData
```
