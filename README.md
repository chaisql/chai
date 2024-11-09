# ChaiSQL

ChaiSQL is a modern embedded SQL database, focusing on flexibility and ease of use for developers.

[![Build Status](https://github.com/chaisql/chai/actions/workflows/go.yml/badge.svg)](https://github.com/chaisql/chai/actions/workflows/go.yml)
[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white&style=flat-square)](https://pkg.go.dev/github.com/chaisql/chai)
![Status](https://img.shields.io/badge/Project%20Stage-Development-yellow)

## Key Features

- **PostgreSQL API**: ChaiSQL SQL API is compatible with PostgreSQL
- **Optimized for Go**: Native Go implementation with no CGO dependency.
- **Storage flexibility**: Store data on-disk or in-memory.
- **Solid foundations**: ChaiSQL is backed by [Pebble](https://github.com/cockroachdb/pebble) for native Go toolchains, and [RocksDB](https://rocksdb.org/) for non-Go or CGO builds (coming soon).

## Roadmap

ChaiSQL is work in progress and is not ready yet for production.

Here is a high level list of features that we want to implement in the near future, in no particular order:

- [ ] Stable storage format (90% completed)
- [ ] Implement most of the SQL-92 standard (detailed roadmap coming soon)
- [ ] Provide clients for other languages (JS/TS, Python, etc) and add support for RocksDB as the backend
- [ ] Compatibility with PostgreSQL drivers and ORMs

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

    err = db.Exec(`
        CREATE TABLE user (
            id              INT         PRIMARY KEY,
            name            TEXT        NOT NULL UNIQUE,
            age             INT         NOT NULL,
            created_at      TIMESTAMP
        )
    `)

    err = db.Exec(`INSERT INTO user (id, name, age) VALUES (1, "Jo Bloggs", 33)`)

    rows, err := db.Query("SELECT id, name, age, address FROM user WHERE age >= 18")
    defer rows.Close()

    err = rows.Iterate(func(r *chai.Row) error {
        // scan each column
        var id, age int
        var name string
        err = r.Scan(&id, &name, &age)
        // or into a struct
        type User struct {
            ID   int
            Name string
            Age  int
        }
        var u User
        err = r.StructScan(&u)
        // or even a map
        m := make(map[string]any)
        err = r.MapScan(&m)
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
