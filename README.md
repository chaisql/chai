<h1 align="center"> Genji </h1>
<p align="center">
  <a href="https://genji.dev">
    <img alt="Genji" title="Genji" src="https://raw.githubusercontent.com/genjidb/docs/master/assets/icons/logo.svg?sanitize=true" width="100">
  </a>
</p>

<p align="center">
  Document-oriented, embedded, SQL database
</p>

## Introduction

[![Build Status](https://travis-ci.org/genjidb/genji.svg)](https://travis-ci.org/genjidb/genji)
[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white&style=flat-square)](https://pkg.go.dev/github.com/genjidb/genji)
[![Slack channel](https://img.shields.io/badge/slack-join%20chat-green.svg)](https://gophers.slack.com/messages/CKPCYQFE0)
![Fuzz](https://github.com/genjidb/genji/workflows/Fuzz/badge.svg)

Genji is a schemaless database that allows running SQL queries on documents.

Checkout the [SQL documentation](https://genji.dev/docs/genji-sql), the [Go doc](https://pkg.go.dev/github.com/genjidb/genji) and the [usage example](#usage) in the README to get started quickly.

> :warning: **Genji's API is still unstable**: Database compatibility is not guaranteed before reaching v1.0.0

## Features

- **Optional schemas**: Genji tables are schemaless, but it is possible to add constraints on any field to ensure the coherence of data within a table.
- **Multiple Storage Engines**: It is possible to store data on disk or in ram, but also to choose between B-Trees and LSM trees. Genji relies on [BoltDB](https://github.com/etcd-io/bbolt) and [Badger](https://github.com/dgraph-io/badger) to manage data.
- **Transaction support**: Read-only and read/write transactions are supported by default.
- **SQL and Documents**: Genji mixes the best of both worlds by combining powerful SQL commands with JSON.
- **Easy to use, easy to learn**: Genji was designed for simplicity in mind. It is really easy to insert and read documents of any shape.
- **Compatible** with the `database/sql` package

## Installation

Install the Genji database

```bash
go get github.com/genjidb/genji
```

## Usage

There are two ways of using Genji, either by using Genji's API or by using the [`database/sql`](https://golang.org/pkg/database/sql/) package.

### Using Genji's API

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/genjidb/genji"
    "github.com/genjidb/genji/document"
)

func main() {
    // Create a database instance, here we'll store everything on-disk using the BoltDB engine
    db, err := genji.Open("my.db")
    if err != nil {
        log.Fatal(err)
    }
    // Don't forget to close the database when you're done
    defer db.Close()

    // Attach context, e.g. (*http.Request).Context().
    db = db.WithContext(context.Background())

    // Create a table. Schemas are optional, you don't need to specify one if not needed
    err = db.Exec("CREATE TABLE user")

    // Create an index
    err = db.Exec("CREATE INDEX idx_user_name ON test (name)")

    // Insert some data
    err = db.Exec("INSERT INTO user (id, name, age) VALUES (?, ?, ?)", 10, "Foo1", 15)

    // Supported values can go from simple integers to richer data types like lists or documents
    err = db.Exec(`
    INSERT INTO user (id, name, age, address, friends)
    VALUES (
        11,
        'Foo2',
        20,
        {"city": "Lyon", "zipcode": "69001"},
        ["foo", "bar", "baz"]
    )`)

    // Go structures can be passed directly
    type User struct {
        ID              uint
        Name            string
        TheAgeOfTheUser float64 `genji:"age"`
        Address         struct {
            City    string
            ZipCode string
        }
    }

    // Let's create a user
    u := User{
        ID:              20,
        Name:            "foo",
        TheAgeOfTheUser: 40,
    }
    u.Address.City = "Lyon"
    u.Address.ZipCode = "69001"

    err = db.Exec(`INSERT INTO user VALUES ?`, &u)

    // Query some documents
    res, err := db.Query("SELECT id, name, age, address FROM user WHERE age >= ?", 18)
    // always close the result when you're done with it
    defer res.Close()

    // Iterate over the results
    err = res.Iterate(func(d types.Document) error {
        // When querying an explicit list of fields, you can use the Scan function to scan them
        // in order. Note that the types don't have to match exactly the types stored in the table
        // as long as they are compatible.
        var id int
        var name string
        var age int32
        var address struct {
            City    string
            ZipCode string
        }

        err = document.Scan(d, &id, &name, &age, &address)
        if err != nil {
            return err
        }

        fmt.Println(id, name, age, address)

        // It is also possible to scan the results into a structure
        var u User
        err = document.StructScan(d, &u)
        if err != nil {
            return err
        }

        fmt.Println(u)

        // Or scan into a map
        var m map[string]interface{}
        err = document.MapScan(d, &m)
        if err != nil {
            return err
        }

        fmt.Println(m)
        return nil
    })
}

```

### Using database/sql

```go
// import Genji as a blank import
import _ "github.com/genjidb/genji/driver"

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

## Engines

Genji currently supports storing data in [BoltDB](https://github.com/etcd-io/bbolt), [Badger](https://github.com/dgraph-io/badger) and in-memory.

### Using the BoltDB engine

```go
import (
    "log"

    "github.com/genjidb/genji"
)

func main() {
    db, err := genji.Open("my.db")
    defer db.Close()
}
```

### Using the memory engine

```go
import (
    "log"

    "github.com/genjidb/genji"
)

func main() {
    db, err := genji.Open(":memory:")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
}
```

### Using the Badger engine

First install the module

```bash
go get github.com/genjidb/genji/engine/badgerengine
```

```go
import (
    "context"
    "log"

    "github.com/genjidb/genji"
    "github.com/genjidb/genji/engine/badgerengine"
    "github.com/dgraph-io/badger/v2"
)

func main() {
    // Create a badger engine
    ng, err := badgerengine.NewEngine(badger.DefaultOptions("mydb"))
    if err != nil {
        log.Fatal(err)
    }

    // Pass it to genji
    db, err := genji.New(context.Background(), ng)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
}
```

## Genji shell

The genji command line provides an SQL shell that can be used to create, modify and consult Genji databases.

Make sure the Genji command line is installed:

```bash
go get github.com/genjidb/genji/cmd/genji
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

## Contributing

Contributions are welcome!

See [ARCHITECTURE.md](./ARCHITECTURE.md) and [CONTRIBUTING.md](.github/CONTRIBUTING.md).

Thank you, [contributors](https://github.com/genjidb/genji/graphs/contributors)!

If you have any doubt, join the [Gophers Slack channel](https://gophers.slack.com/messages/CKPCYQFE0) or open an [issue](https://github.com/genjidb/genji/issues/new).
