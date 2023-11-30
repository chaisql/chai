<h1 align="center"> ChaiSQL </h1>
<p align="center">
  <a href="https://chai.dev">
    <img alt="chaiSQL" title="chaiSQL" src="https://raw.githubusercontent.com/chaisql/docs/master/assets/icons/logo.svg?sanitize=true" width="100">
  </a>
</p>

<p align="center">
  ChaiSQL is a modern embedded SQL database, focusing on flexibility and ease of use for developers. It provides a fresh alternative to traditional SQL databases by supporting more flexible schemas and offering advanced features tailored for modern applications.
</p>


[![Build Status](https://github.com/chaisql/chai/actions/workflows/go.yml/badge.svg)](https://github.com/chaisql/chai/actions/workflows/go.yml)
[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white&style=flat-square)](https://pkg.go.dev/github.com/chaisql/chai)
![Status](https://img.shields.io/badge/status-alpha-yellow)


## Key Features

- **Modern SQL Experience**: ChaiSQL introduces a modern twist to SQL, offering enhanced features and performance optimizations.
- **Schema Flexibility**: Support for strict, partial, and schemaless table designs, catering to various data modeling needs.
- **Optimized for Go**: Native Go implementation with no CGO dependency.
- **SQLite Alternative**: Aims to be a modern alternative to SQLite.

## Installation

Install the chai database

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

> :warning: Chai's API is still evolving: We are working towards the v1.0.0 release, which will bring stability to the database structure and API.

Checkout the [SQL documentation](https://chai.dev/docs/essentials/sql-introduction/), the [Go doc](https://pkg.go.dev/github.com/chaisql/chai) and the [usage example](#usage) in the README to get started quickly.

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

For any questions or discussions, join our [Gophers Slack channel](https://gophers.slack.com/messages/CKPCYQFE0) or open an [issue](https://github.com/chaisql/chai/issues/new).
