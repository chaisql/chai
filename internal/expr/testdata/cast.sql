-- test: source(INT)
> CAST (1 AS INTEGER)
1

> CAST (1 AS BOOL)
true

> CAST (1 AS DOUBLE)
1.0

> CAST (1 AS TEXT)
'1'

! CAST (1 AS BLOB)
'cannot cast integer as blob'

! CAST (1 AS ARRAY)
'cannot cast integer as array'

! CAST (1 AS DOCUMENT)
'cannot cast integer as document'

-- test: source(DOUBLE)
> CAST (1.1 AS DOUBLE)
1.1

> CAST (1.1 AS INTEGER)
1

! CAST (1.1 AS BOOL)
'cannot cast double as bool'

> CAST (1.1 AS TEXT)
'1.1'

! CAST (1.1 AS BLOB)
'cannot cast double as blob'

! CAST (1.1 AS ARRAY)
'cannot cast double as array'

! CAST (1.1 AS DOCUMENT)
'cannot cast double as document'

-- test: source(BOOL)
> CAST (true AS BOOL)
true

> CAST (true AS INTEGER)
1

> CAST (false AS INTEGER)
0

! CAST (true AS DOUBLE)
'cannot cast bool as double'

> CAST (true AS TEXT)
'true'

! CAST (true AS BLOB)
'cannot cast bool as blob'

! CAST (true AS ARRAY)
'cannot cast bool as array'

! CAST (true AS DOCUMENT)
'cannot cast bool as document'

-- test: source(TEXT)
> CAST ('a' AS TEXT)
'a'

> CAST ('100' AS INTEGER)
100

> CAST ('100.5' AS INTEGER)
100

! CAST ('a' AS INTEGER)

> CAST ('3.14' AS DOUBLE)
3.14

> CAST ('3' AS DOUBLE)
3.0

! CAST ('10.5wdwd' AS DOUBLE)

> CAST ('true' AS BOOL)
true

> CAST ('false' AS BOOL)
false

! CAST ('falSe' AS BOOL)

> CAST ('YXNkaW5l' AS BLOB)
'\x617364696e65'

> CAST ('[]' AS ARRAY)
[]

> CAST ('[1, true, [], {" a": 1}]' AS ARRAY)
[1, true, [], {" a": 1}]

! CAST ('[1, true, [], {" a": 1}' AS ARRAY)

> CAST ('{"a": 1, "b": [1, true, [], {" a": 1}]}' AS DOCUMENT)
{"a": 1, "b": [1, true, [], {" a": 1}]}

! CAST ('{"a": 1' AS DOCUMENT)

-- test: source(BLOB)
> CAST ('\xAF' AS BLOB)
'\xAF'

! CAST ('\xAF' AS INT)
'cannot cast blob as integer'

! CAST ('\xAF' AS DOUBLE)
'cannot cast blob as double'

> CAST ('\x617364696e65' AS TEXT)
'YXNkaW5l'

! CAST ('\xAF' AS ARRAY)
'cannot cast blob as array'

! CAST ('\xAF' AS DOCUMENT)
'cannot cast blob as document'

-- test: source(ARRAY)
> CAST ([1] AS ARRAY)
[1]

! CAST ([1] AS INTEGER)
'cannot cast array as integer'

! CAST ([1] AS DOUBLE)
'cannot cast array as double'

> CAST ([1, true, [], {" a": 1}] AS TEXT)
'[1, true, [], {" a": 1}]'

! CAST ([1] AS BLOB)
'cannot cast array as blob'

! CAST ([1] AS DOCUMENT)
'cannot cast array as document'

-- test: source(DOCUMENT)
> CAST ({a: 1} AS DOCUMENT)
{a: 1}

! CAST ({a: 1} AS INTEGER)
'cannot cast document as integer'

! CAST ({a: 1} AS DOUBLE)
'cannot cast document as double'

> CAST ({"a": 1, "b": [1, true, [], {" a": 1}]} AS TEXT)
'{"a": 1, "b": [1, true, [], {" a": 1}]}'

! CAST ({a: 1} AS BLOB)
'cannot cast document as blob'

! CAST ({a: 1} AS ARRAY)
'cannot cast document as array'