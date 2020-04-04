---
title: "Data Types"
date: 2020-03-29T17:44:36+04:00
weight: 20
description: >
  This chapter describes the list of data types
---

Genji provides a list of simple data types to store and manipulate data.
There are basically two kinds of data types:

* *Fixed size data types*: Types who use a fixed amount of space regardless of the value stored in them.
* *Variable size data types*: Types who take more or less space depending on the value stored in them.

## Fixed size data types

| Name    | Description                     | From                     | To                      |
| ------ | ------------------------------ | ----------------------- | ---------------------- |
| int8    | 1 byte signed integer           | -128                     | 127                     |
| int16   | 2 bytes signed integer          | -32768                   | 32767                   |
| int32   | 4 bytes signed integer          | -2147483648              | 2147483647              |
| int64   | 8 bytes signed integer          | -9223372036854775808     | 9223372036854775807     |
| float64 | 8 bytes decimal                 | -1.7976931348623157e+308 | 1.7976931348623157e+308 |
| bool    | Can be either `true` or `false` | `false` | `true` |

## Variable size data types

| Name | Description |
| --- | --- |
| int | Signed integer which takes 1, 2, 4 or 8 bytes depending on the size of the stored number |
| integer | Alias for `int` |
| duration | Represents a length of time in nanoseconds. Stored as an integer |
| blob | Variable size blob of data |
| text | Variable size UTF-8 encoded string |
| array | Array of values of any type |
| document | Object that contains pairs that associate a string field to a value of any type |

## The case of NULL

In Genji, *Null* is treated as both a value and a type. It represents the absence of data, and is returned in various cases:

* when selecting a field that doesn't exists
* when selecting a field whose value is null
* as the result of the evaluation of an expression

## Conversion

Whenever Genji needs to manipulate data of different types, depending on the situation it will rely on either:

* **explicit conversion**: The source type and destination type are clearly identified. Ex: When inserting data to field with a constraint or when doing a `CAST`.
* **implicit conversion**: Two values of different types need to be compared or used by an operator during the evaluation of an [expression]({{< relref "/docs/genji-sql/expressions.md" >}})

### Explicit conversion

Explicit conversion is used when we want to convert a value of a *source* type into a *target* type.
However, Genji types are not all compatible with one another, and when a user tries to convert them, Genji returns an error.
Here is a table describing type compatibility.

| Source type | Target type | Converted                                      |
| ---------- | --------------- | --------------------------------------------- |
| any integer | float64          | yes                                            |
| any integer | text           | no                                             |
| any integer | blob            | no                                             |
| any integer | bool             | yes, `false` if zero, otherwise `true` |
| float64     | any integer      | yes, if not lossy                              |
| float64     | text           | no                                             |
| float64     | blob            | no                                             |
| float64     | bool             | yes, `false` if zero, otherwise `true` |
| text      | any integer      | no                                             |
| text      | float64          | no                                             |
| text      | blob            | yes                                            |
| text      | bool             | yes, `false` if empty string, otherwise `true` |
| blob       | any integer      | no                                             |
| blob       | float64          | no                                             |
| blob       | text           | yes                                            |
| blob       | bool             | yes, `false` if empty, otherwise `true` |
| bool        | any integer      | yes                                            |
| bool        | float64          | yes                                            |
| bool        | text           | no                                             |
| bool        | blob            | no                                             |
| null | any type | yes, the zero value of the type |

Arrays and documents cannot be converted to any other values.

### Implicit conversion

Implicit conversion usually takes place during the evaluation of an [expression]({{< relref "/docs/genji-sql/expressions" >}}). Different rules may apply depending on the expression kind. Comparing values, evaluating literals, using arithmetic operators, all have their own set of implicit conversion rules.
