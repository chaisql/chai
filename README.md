# ChaiSQL

ChaiSQL is a modern embedded SQL database, focusing on flexibility and ease of use for developers. It provides a fresh alternative to traditional embedded databases by offering advanced features tailored for modern applications.

[![Build Status](https://github.com/chaisql/chai/actions/workflows/go.yml/badge.svg)](https://github.com/chaisql/chai/actions/workflows/go.yml)
[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white&style=flat-square)](https://pkg.go.dev/github.com/chaisql/chai)
![Status](https://img.shields.io/badge/status-alpha-yellow)

> :warning: ChaiSQL is taking a new direction (see [dev](https://github.com/chaisql/chai/tree/dev) branch) and next release is expected to contain lots of breaking changes in the API and data format. Do NOT use in production.

## Key Features

- **Modern SQL Experience**: ChaiSQL introduces a modern twist to traditional SQL embedded databases
- **Optimized for Go**: Native Go implementation with no CGO dependency.
- **Solid foundations**: ChaiSQL is backed by [Pebble](https://github.com/cockroachdb/pebble) for native Go toolchains, and [RocksDB](https://rocksdb.org/) for non-Go or CGO builds (coming soon).
- **Schema Flexibility**: Support for strict, partial, and schemaless table designs, catering to various data modeling needs.

## Roadmap

ChaiSQL is work in progress and is not ready yet for production.

Here is a high level list of features that we want to implement in the near future, in no particular order:

- [ ] Stable storage format (90% completed)
- [ ] Implement most of the SQL-92 standard (detailed roadmap coming soon)
- [ ] Provide clients for other languages (JS/TS, Python, etc) and add support for RocksDB as the backend

## Installation

Install the ChaiSQL database

```bash
go install github.com/chaisql/chai
```

## Quickstart

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/chaisql/chai"
)

func main() {
    // Create a database instance, here we'll store everything on-disk
    db, err := chai.Open("mydb")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Create a table.
    // Notice that it is possible to define constraints on nested columns.
    err = db.Exec(`
        CREATE TABLE user (
            id              INT         PRIMARY KEY,
            name            TEXT        NOT NULL UNIQUE,
            created_at      TIMESTAMP   NOT NULL,
            address (
                city        TEXT    DEFAULT "unknown",
                zipcode     TEXT
            ),
            friends         ARRAY,

            CHECK len(friends) > 0
        )
    `)

    err = db.Exec(`
        INSERT INTO user (id, name, age, address, friends)
        VALUES (
            11,
            'Foo2',
            20,
            {city: "Lyon", zipcode: "69001"},
            ["foo", "bar", "baz"]
        )`)

    // Go structures can be passed directly
    type User struct {
        ID              uint
        Name            string
        TheAgeOfTheUser float64 `chai:"age"`
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

    // Query data
    rows, err := db.Query("SELECT id, name, age, address FROM user WHERE age >= ?", 18)
    defer rows.Close()

    err = rows.Iterate(func(r *chai.Row) error {
        err = r.Scan(...)
        err = r.StructScan(...)
        err = r.MapScan(...)
        return nil
    })
}
```

Checkout the [Go doc](https://pkg.go.dev/github.com/chaisql/chai) and the [usage example](#usage) in the README to get started quickly.

### In-memory database

For in-memory operations, simply use `:memory:`:

```go
db, err := chai.Open(":memory:")
```

### Using database/sql

```go
// import chai as a blank import
import _ "github.com/chaisql/chai/driver"

// Create a sql/database DB instance
db, err := sql.Open("chai", "mydb")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Then use db as usual
res, err := db.ExecContext(...)
res, err := db.Query(...)
res, err := db.QueryRow(...)

// use the driver.Scanner to scan into a struct
var u User
err = res.Scan(driver.Scanner(&u))
```

## chai shell

The chai command line provides an SQL shell for database management:

```bash
go install github.com/chaisql/chai/cmd/chai@latest
```

Usage example:

```bash
# For in-memory database:
chai

# For disk-based database:
chai dirName
```

## Contributing

Contributions are welcome!

A big thanks to our [contributors](https://github.com/chaisql/chai/graphs/contributors)!

<a href="https://github.com/chaisql/chai/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=chaisql/chai" />
</a>

Made with [contrib.rocks](https://contrib.rocks).

For any questions or discussions, open an [issue](https://github.com/chaisql/chai/issues/new).
