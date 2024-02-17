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

-- test: source(BOOL)
> CAST (true AS BOOL)
true

> CAST (true AS INTEGER)
1

> CAST (false AS INTEGER)
0

! CAST (true AS DOUBLE)
'cannot cast boolean as double'

> CAST (true AS TEXT)
'true'

! CAST (true AS BLOB)
'cannot cast boolean as blob'

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

-- test: source(BLOB)
> CAST ('\xAF' AS BLOB)
'\xAF'

! CAST ('\xAF' AS INT)
'cannot cast blob as integer'

! CAST ('\xAF' AS DOUBLE)
'cannot cast blob as double'

> CAST ('\x617364696e65' AS TEXT)
'YXNkaW5l'
