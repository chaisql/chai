# ChaiSQL

ChaiSQL is a modern, embedded SQL database with a PostgreSQL-compatible API, written in pure Go.
It’s designed for developers who want the power of SQL with the simplicity of an embedded database.

[![Build Status](https://github.com/chaisql/chai/actions/workflows/go.yml/badge.svg)](https://github.com/chaisql/chai/actions/workflows/go.yml)
[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white&style=flat-square)](https://pkg.go.dev/github.com/chaisql/chai)
![Status](https://img.shields.io/badge/Project%20Stage-Development-yellow)

## Key Features

- **PostgreSQL API Compatibility** – Run familiar SQL queries with minimal changes.
- **Pure Go Implementation** – No CGO or external dependencies.
- **Flexible Storage** – Choose between on-disk or in-memory modes.
- **Backed by [Pebble](https://github.com/cockroachdb/pebble)** – Rock-solid storage engine from CockroachDB.

## Roadmap

ChaiSQL is still in active development and not production-ready. Planned features:

- [ ] Stable storage format – finalize and stabilize (90% complete)
- [ ] SQL-92 coverage – implement core + advanced features (detailed roadmap soon)
- [ ] Language clients – official drivers for JS/TS, Python, etc.
- [ ] Backend flexibility – RocksDB support alongside Pebble
- [ ] ORM & driver compatibility – seamless use with PostgreSQL ecosystem

## Installation

Install the ChaiSQL driver and CLI:

```bash
go install github.com/chaisql/chai@latest
go install github.com/chaisql/chai/cmd/chai@latest
```

## Quickstart

Here’s a simple Go example that creates a table, inserts rows, and queries them:

```go
package main

import (
    "database/sql"
    "fmt"
    "log"

    _ "github.com/chaisql/chai"
)

func main() {
    // Open an on-disk database called "mydb"
    db, err := sql.Open("chai", "mydb")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Create schema
    _, err = db.Exec(`
        CREATE TABLE users (
            id          SERIAL PRIMARY KEY,
            name        TEXT NOT NULL UNIQUE,
            email       TEXT NOT NULL,
            age         INT  NOT NULL,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    `)
    if err != nil {
        log.Fatal(err)
    }

    // Insert some data
    _, err = db.Exec(`
        INSERT INTO users (name, email, age)
        VALUES
            ('Alice', 'alice@example.com', 30),
            ('Bob',   'bob@example.com',   25),
            ('Carol', 'carol@example.com', 40);
    `)
    if err != nil {
        log.Fatal(err)
    }

    // Query active adults
    rows, err := db.Query(`
        SELECT id, name, email, age
        FROM users
        WHERE age >= 18
        ORDER BY age DESC
    `)
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    for rows.Next() {
        var id, age int
        var name, email string
        if err := rows.Scan(&id, &name, &email, &age); err != nil {
            log.Fatal(err)
        }
        fmt.Printf("User %d: %s (%s), %d years old\n", id, name, email, age)
    }
}
```

### In-memory Database

For ephemeral databases, just use `:memory:`:

```go
db, err := sql.Open("chai", ":memory:")
```

## Chai shell

The chai command-line tool provides an interactive SQL shell:

```bash
# In-memory database:
chai

# Disk-based database:
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

## ❓ FAQ

### Why not just use SQLite?

SQLite is fantastic, but it has its own SQL dialect. ChaiSQL is designed for PostgreSQL compatibility, so it feels familiar if you already use Postgres.

### Is it production-ready?

Not yet. We’re actively building out SQL support and stability.

### Can I use existing Postgres tools?

Yes, ChaiSQL aims to be compatible with PostgreSQL drivers and ORMs.
