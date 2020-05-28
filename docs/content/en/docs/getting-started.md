---
title: "Getting started"
linkTitle: "Getting started"
date: 2020-03-29T17:22:52+04:00
weight: 20
description: >
  How to install and use Genji
---

## Prerequisites

Genji requires at least Go 1.12.

## Installation

To install the Genji database, run this command:

```bash
go get github.com/genjidb/genji
```

## Golang API documentation

To learn how to embed Genji in your Go code, follow the instructions in the [Go package documentation](https://pkg.go.dev/github.com/genjidb/genji@v0.5.0?tab=doc).

## Try it out!

To try Genji without writing code, you can use the Genji command-line shell.

First, install it:

```bash
go get github.com/genjidb/genji/cmd/genji
```

To open an in-memory database, simply type:

```bash
genji
```

You can then enter your [SQL queries]({{< relref "/docs/getting-started" >}}) directly in the shell.

It is also possible to create an on-disk database, using either [BoltDB](https://github.com/etcd-io/bbolt) or [Badger](https://github.com/dgraph-io/badger).

### On-disk database using BoltDB

Run the `genji` command followed by the name of the database file of your choice.

```bash
genji my.db
```

### On-disk database using Badger

Run the following command by replacing `pathToDBDir` by the directory of your choice.

```bash
genji --badger pathToDBDir
```

## Next step

Once Genji is setup, follow the [Genji SQL]({{< relref "/docs/genji-sql/_index.md" >}}) chapter to learn how to run queries.
