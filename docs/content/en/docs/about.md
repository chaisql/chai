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

* **Optional schemas**: Genji tables are schemaless by default, but it is possible to add constraints on any field to ensure the coherence of data within a table.
* **Multiple Storage Engines**: It is possible to store data on disk or in ram, but also to choose between B-Trees and LSM trees. Genji relies on BoltDB and Badger to manage data.
* **Transaction support**: Read-only and read/write transactions are supported by default.

