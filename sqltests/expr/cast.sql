-- test: source(INT)
> CAST (1 AS INTEGER)
1

> CAST (1 AS BOOL)
true

> CAST (1 AS DOUBLE PRECISION)
1.0

> CAST (1 AS TEXT)
'1'

! CAST (1 AS BYTEA)
'cannot cast "integer" as "bytea"'

-- test: source(DOUBLE PRECISION)
> CAST (1.1 AS DOUBLE PRECISION)
1.1

> CAST (1.1 AS INTEGER)
1

! CAST (1.1 AS BOOL)
'cannot cast "double precision" as "boolean"'

> CAST (1.1 AS TEXT)
'1.1'

! CAST (1.1 AS BYTEA)
'cannot cast "double precision" as "bytea"'

-- test: source(BOOL)
> CAST (true AS BOOL)
true

> CAST (true AS INTEGER)
1

> CAST (false AS INTEGER)
0

! CAST (true AS DOUBLE PRECISION)
'cannot cast "boolean" as "double precision"'

> CAST (true AS TEXT)
'true'

! CAST (true AS BYTEA)
'cannot cast "boolean" as "bytea"'

-- test: source(TEXT)
> CAST ('a' AS TEXT)
'a'

> CAST ('100' AS INTEGER)
100

> CAST ('100.5' AS INTEGER)
100

! CAST ('a' AS INTEGER)

> CAST ('3.14' AS DOUBLE PRECISION)
3.14

> CAST ('3' AS DOUBLE PRECISION)
3.0

! CAST ('10.5wdwd' AS DOUBLE PRECISION)

> CAST ('true' AS BOOL)
true

> CAST ('false' AS BOOL)
false

> CAST ('falSe' AS BOOL)
false

> CAST ('0' AS BOOL)
false

> CAST ('1' AS BOOL)
true

> CAST ('t' AS BOOL)
true

> CAST ('f' AS BOOL)
false

> CAST ('TrUe' AS BOOL)
true

> CAST ('yes' AS BOOL)
true

> CAST ('y' AS BOOL)
true

> CAST ('no' AS BOOL)
false

> CAST ('n' AS BOOL)
false

> CAST ('on' AS BOOL)
true

> CAST ('off' AS BOOL)
false

> CAST ('YXNkaW5l' AS BYTEA)
'\x617364696e65'

-- test: source(BYTEA)
> CAST ('\xAF' AS BYTEA)
'\xAF'

! CAST ('\xAF' AS INT)
'cannot cast "bytea" as "integer"'

! CAST ('\xAF' AS DOUBLE PRECISION)
'cannot cast "bytea" as "double precision"'

> CAST ('\x617364696e65' AS TEXT)
'YXNkaW5l'

-- test: short form casts (::)
-- Source: INT
> 1::INTEGER
1

> 1::BOOL
true

> 1::DOUBLE PRECISION
1.0

> 1::TEXT
'1'

! 1::BYTEA
'cannot cast "integer" as "bytea"'

-- Source: DOUBLE PRECISION
> 1.1::DOUBLE PRECISION
1.1

> 1.1::INTEGER
1

! 1.1::BOOL
'cannot cast "double precision" as "boolean"'

> 1.1::TEXT
'1.1'

! 1.1::BYTEA
'cannot cast "double precision" as "bytea"'

-- Source: BOOL
> true::BOOL
true

> true::INTEGER
1

> false::INTEGER
0

! true::DOUBLE PRECISION
'cannot cast "boolean" as "double precision"'

> true::TEXT
'true'

! true::BYTEA
'cannot cast "boolean" as "bytea"'

-- Source: TEXT
> 'a'::TEXT
'a'

> '100'::INTEGER
100

> '100.5'::INTEGER
100

! 'a'::INTEGER

> '3.14'::DOUBLE PRECISION
3.14

> '3'::DOUBLE PRECISION
3.0

! '10.5wdwd'::DOUBLE PRECISION

> 'true'::BOOL
true

> 'false'::BOOL
false

> 'falSe'::BOOL
false

> '0'::BOOL
false

> '1'::BOOL
true

> 't'::BOOL
true

> 'f'::BOOL
false

> 'TrUe'::BOOL
true

> 'yes'::BOOL
true

> 'y'::BOOL
true

> 'no'::BOOL
false

> 'n'::BOOL
false

> 'on'::BOOL
true

> 'off'::BOOL
false

> 'YXNkaW5l'::BYTEA
'\x617364696e65'

-- Source: BYTEA
> '\xAF'::BYTEA
'\xAF'

! '\xAF'::INT
'cannot cast "bytea" as "integer"'

! '\xAF'::DOUBLE PRECISION
'cannot cast "bytea" as "double precision"'

> '\x617364696e65'::TEXT
'YXNkaW5l'

-- Additional / edge cases
> -1::INTEGER
-1

> (1 + 2)::DOUBLE PRECISION
3.0

> (1 + 2)::DOUBLE PRECISION::TEXT
'3'

> 100::INTEGER::DOUBLE PRECISION
100.0

> (1)::DOUBLE PRECISION::TEXT
'1'

> (1.345)::DOUBLE PRECISION::TEXT
'1.345'

-- function-related tests for ::
-- LOWER / UPPER with short-form casts
> LOWER('HeLLo'::TEXT)
'hello'

> UPPER('HeLLo'::TEXT)
'HELLO'

-- function applied to a casted numeric expression
> LOWER((1 + 2)::TEXT)
'3'

-- concatenation with short-form casts
> 'a'::TEXT || 'B'::TEXT
'aB'

-- chaining cast after a function result (function returns text so cast is no-op)
> LOWER('MiXeD'::TEXT)::TEXT
'mixed'

-- wildcard cannot be cast
! *::TEXT
'expected EOF, got ::'
