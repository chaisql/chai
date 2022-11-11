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

[![Build Status](https://github.com/genjidb/genji/actions/workflows/go.yml/badge.svg)](https://github.com/genjidb/genji/actions/workflows/go.yml)
[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white&style=flat-square)](https://pkg.go.dev/github.com/genjidb/genji)
![Status](https://img.shields.io/badge/status-alpha-yellow)

Genji is a database that allows running SQL queries on documents.

Checkout the [SQL documentation](https://genji.dev/docs/essentials/sql-introduction/), the [Go doc](https://pkg.go.dev/github.com/genjidb/genji) and the [usage example](#usage) in the README to get started quickly.

> :warning: **Genji's API is still unstable**: Database compatibility is not guaranteed before reaching v1.0.0

## Features

- **SQL and documents**: Use a powerful SQL language designed for documents as first-class citizen.
- **Flexible schemas**: Define your table with strict schemas, partial schemas, or no schemas at all.
- **Transaction support**: Fully serializable transactions with multiple readers and single writer. Readers don’t block writers and writers don’t block readers.
- **Compatible** with the `database/sql` package

## Installation

Install the Genji database

```bash
go install github.com/genjidb/genji
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
    "github.com/genjidb/genji/types"
)

func main() {
    // Create a database instance, here we'll store everything on-disk
    db, err := genji.Open("mydb")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // If needed, attach context, e.g. (*http.Request).Context().
    db = db.WithContext(context.Background())

    // Create a table with a strict schema.
    // Useful to have full control of the table content.
    // Notice that it is possible to define constraint on nested documents.
    err = db.Exec(`
        CREATE TABLE user (
            id              INT     PRIMARY KEY,
            name            TEXT    NOT NULL UNIQUE,
            address (
                city        TEXT    DEFAULT "?",
                zipcode     TEXT
            ),
            friends         ARRAY
        )
    `)

    // or a partial schema, using an ellipsis.
    // Useful to apply constraints only on a few fields, while storing documents of any shape
    err = db.Exec(`
        CREATE TABLE github_issues (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            state TEXT NOT NULL,
            ...
        );

        CREATE INDEX ON github_issues (state);
    `)

    // or a schemaless table
    // Useful when you need to store data first and explore it later,
    // or if you the structure of the data is already defined somewhere else
    // (e.g. documents returned from an API)
    err = db.Exec(`CREATE TABLE twitter_tweets_v2`)

    // Create an index
    err = db.Exec("CREATE INDEX user_city_idx ON user (address.city, address.zipCode)")

    // Insert some data
    err = db.Exec("INSERT INTO user (id, name) VALUES (?, ?)", 10, "Foo1", 15)

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

### In-memory database

To store data in memory, use `:memory:` instead of a database path:

```go
db, err := genji.Open(":memory:")
```

### Using database/sql

```go
// import Genji as a blank import
import _ "github.com/genjidb/genji/driver"

// Create a sql/database DB instance
db, err := sql.Open("genji", "mydb")
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

## Genji shell

The genji command line provides an SQL shell that can be used to create, modify and consult Genji databases.

Make sure the Genji command line is installed:

```bash
go install github.com/genjidb/genji/cmd/genji@latest
```

Example:

```bash
# Opening an in-memory database:
genji

# Opening a database on disk:
genji dirName
```

## Contributing

Contributions are welcome!

Thank you, [contributors](https://github.com/genjidb/genji/graphs/contributors)!

<a href="https://github.com/genjidb/genji/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=genjidb/genji" />
</a>

Made with [contrib.rocks](https://contrib.rocks).

If you have any doubt, join the [Gophers Slack channel](https://gophers.slack.com/messages/CKPCYQFE0) or open an [issue](https://github.com/genjidb/genji/issues/new).
