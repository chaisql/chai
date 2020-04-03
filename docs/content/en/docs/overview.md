---
title: "Overview"
linkTitle: "Overview"
date: 2020-03-29T17:22:52+04:00
weight: 10
description: >
  An introduction to Genji
---

## What is Genji?

Genji is an embedded database written in Go that aims to simplify dealing with data in the modern world.
It combines the power of **SQL** with the versatility of **documents** to provide a maximum of flexibility with no compromise.

Here is a list of Genji's main features:

* **Optional schemas**: Genji tables are schemaless, but it is possible to add constraints on any field to ensure the coherence of data within a table.
* **Multiple Storage Engines**: It is possible to store data on disk or in ram, but also to choose between B-Trees and LSM trees. Genji relies on [BoltDB](https://github.com/etcd-io/bbolt) and [Badger](https://github.com/dgraph-io/badger) to manage data.
* **Transaction support**: Read-only and read/write transactions are supported by default.
* **SQL and Documents**: Genji mixes the best of both worlds by combining powerful SQL commands with JSON *dot notation*.
* **Easy to use, easy to learn**: Genji was designed for simplicity in mind. It is really easy to insert and read documents of any shape.

## Concepts

Genji's main concepts are not new and semantics have been chosen to match as much as possible what is already existing in other databases:

| Classic SQL databases | Genji             |
|-----------------------|-------------------|
| Table                 | Table             |
| Row                   | Document          |
| Column                | Field             |
| Schema                | Field constraints |

* **Table**: A collection of documents. Tables are schemaless by default and support optional
* **Document**: A list of fields
* **Field**: A key-value pair
* **Field constraint**: A constraint validated against a field of every inserted or updated document  

## Next steps

Follow these docs for more information:

* [Getting started]({{< relref "/docs/getting-started" >}})
* [Learn Genji SQL syntax]({{< relref "/docs/genji-sql/_index.md" >}})
