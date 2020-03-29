---
title: "Data Types"
date: 2020-03-29T17:44:36+04:00
weight: 1
description: >
  This chapter describes the list of data types
---

## Fixed size data types

| Name    | Description                     | From                     | To                      |
| :------ | :------------------------------ | :----------------------- | :---------------------- |
| int8    | 1 byte signed integer           | -128                     | 127                     |
| int16   | 2 bytes signed integer          | -32768                   | 32767                   |
| int32   | 4 bytes signed integer          | -2147483648              | 2147483647              |
| int64   | 8 bytes signed integer          | -9223372036854775808     | 9223372036854775807     |
| uint8   | 1 byte unsigned integer         | 0                        | 255                     |
| uint16  | 2 bytes unsigned integer        | 0                        | 65535                   |
| uint32  | 4 bytes unsigned integer        | 0                        | 4294967295              |
| uint64  | 8 bytes unsigned integer        | 0                        | 18446744073709551615    |
| float64 | 8 bytes decimal                 | -1.7976931348623157e+308 | 1.7976931348623157e+308 |
| bool    | Can be either `true` or `false` | `false`                  | `true`                  |

## Variable size data types

| Name     | Description                                                                                |
| :------- | :----------------------------------------------------------------------------------------- |
| int      | Signed integer which takes 1, 2, 4 or 8 bytes depending on the size of the stored number   |
| uint     | Unsigned integer which takes 1, 2, 4 or 8 bytes depending on the size of the stored number |
| integer  | Alias for `int`                                                                            |
| string   | Variable size UTF-8 encoded string                                                         |
| bytes    | Variable size blob of data                                                                 |
| text     | Alias for `string`                                                                         |
| array    | Array of values of any type                                                                |
| document | Object that contains pairs that associate a string field to a value of any type            |

## Conversion

| Source type | Destination type | Converted                                      |
| :---------- | :--------------- | :--------------------------------------------- |
| any integer | float64          | yes                                            |
| any integer | string           | no                                             |
| any integer | bytes            | no                                             |
| any integer | bool             | yes, `false` if zero, otherwise `true`         |
| float64     | any integer      | yes, if not lossy                              |
| float64     | string           | no                                             |
| float64     | bytes            | no                                             |
| float64     | bool             | yes, `false` if zero, otherwise `true`         |
| string      | any integer      | no                                             |
| string      | float64          | no                                             |
| string      | bytes            | yes                                            |
| string      | bool             | yes, `false` if empty string, otherwise `true` |
| bytes       | any integer      | no                                             |
| bytes       | float64          | no                                             |
| bytes       | string           | yes                                            |
| bytes       | bool             | yes, `false` if empty, otherwise `true`        |
| bool        | any integer      | yes                                            |
| bool        | float64          | yes                                            |
| bool        | string           | no                                             |
| bool        | bytes            | no                                             |

## Documents

Genji stores records as documents. A document is an object that contains pairs that associate a string field to a value of any type.
Genji documents are often represented as JSON objects, though they support far more types.

Here is a JSON representation of the structure of a document:

```js
{
    field1: value1,
    field2: value2,
    field3: value3,
    ...
}
```

Each field name must be a string, but values can be of any type, including another document.
