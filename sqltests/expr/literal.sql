-- test: literals/ints
> 1
1

> typeof(1)
'integer'

> -1
-1

> typeof(-1)
'integer'

-- test: literals/bigints
> 100000000000
100000000000

> typeof(100000000000)
'bigint'

> -100000000000
-100000000000

> typeof(-100000000000)
'bigint'

-- test: literals/doubles

> 1.0
1.0

> typeof(1.0)
'double'

> 123456789012345.0
123456789012345.0

> 1234567890123456.0
1.234567890123456e+15

> 1.234567890123456e+15
1.234567890123456e+15

> 1.234567890123456e15
1.234567890123456e+15

> 1.234567890123456e-15
1.234567890123456e-15

> 1.234567890123456E+15
1.234567890123456e+15

-- test: literals/bools

> true
true

> typeof(true)
'boolean'

> tRue
true

> false
false

> fAlse
false

-- test: timestamps
> now()
'2020-01-01T00:00:00Z'

-- test: literals/texts

> 'hello'
'hello'

> typeof('hello')
'text'

> "hello"
'hello'

> typeof("hello")
'text'

-- test: literals/byteas

> '\xFF'
'\xFF'

> typeof('\xFF')
'bytea'

! '\xhello'
'invalid hexadecimal digit: h'
