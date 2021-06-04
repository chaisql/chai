# Genji Architecture

This document sums up the current Genji architecture and will evolve over time.

## Goals

- **SQL first**: SQL is the main way to interact with the database
- **Documents**: anything can be stored, as long as it can be expressed as a document
- **Schema is optional and can be partial**: a table containing documents can have value or structurual constraints on some, all or no fields
- **Run anywhere**: a browser, a low power computer or a server

## Overview

Genji, in a bird's-eye view is structured in the following layers:

- _SQL_, the language and grammar used to express queries to fetch, store and update data
- _Stream_, an internal representation of the low level operation that will be performed on documents by the database
- _Database_, defines the base database components, tables, transactions, indexes
- _Engine_, the storage layer, various KV stores responsible of persiting the data

### Lifecycle of query

1. Genji parses SQL and turns into a Stream

   - the `parser` will use the `scanner` to turn the SQL into a basic, unoptimized `Stream`
   - packages: `driver`, `internal/sql/scanner`, `internal/sql/parser`, `internal/query`, `internal/expr`, `document`

2. the `Planner` will analyze that `Stream` and will optimize it if possible

   - which index to use, removing redundant loads, compute constant expressions, ...
   - packages: `internal/planner`, `internal/stream`

3. That `Stream` will be executed against the `database`, reading and/or modifying indexes, tables

   - packages: `internal/database`

4. interactions with the `database` layer will make calls to the `engine`, to perform reads and writes

   - the `database` layer will encode/decode `document`s into bytes
   - the `engine` layer is responsible of reading or writing bytes in the underlying KV-store
   - packages: `engine/*`, `document/encoding`, `document`

For a description of each those these package, see the [GoDoc](https://pkg.go.dev/github.com/genjidb/genji).

## Current Trade offs

Presently, in the current state of development, the following trade offs are considered:

- _Concurrency_: one writer, multiple readers
  - what: no concurrent writes (serialized), but reads are concurrent
  - why: it simplifies greatly the implementation and avoids dealing with concurrent access, which is supported with varying semantics by the underlying store. A solution would be to implement a MVCC layer within Genji, which is a lot of work and isn't the current priority.
- _Performances_: overall, the focus is more on features than on last mile optimizations
- _SQL standards_: not bothering to respect them
  - what: don't expect Genji's SQL to be portable, it's not.
  - why: documents are really different than rows and colums and Genji's SQL syntax is designed for documents
