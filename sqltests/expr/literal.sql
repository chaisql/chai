-- test: literals/ints
> 1
1

> typeof(1)
'integer'

> -1
-1

> typeof(-1)
'integer'

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

-- test: literals/texts

> 'hello'
'hello'

> typeof('hello')
'text'

> "hello"
'hello'

> typeof("hello")
'text'

-- test: literals/blobs

> '\xFF'
'\xFF'

> typeof('\xFF')
'blob'

! '\xhello'
'invalid hexadecimal digit: h'

-- test: literals/arrays

> [1, true, ['hello'], {a: [1]}]
[1, true, ['hello'], {a: [1]}]

> typeof([1, true, ['hello'], {a: [1]}])
'array'

-- test: literals/documents

> {a: 1}
{a: 1}

> {"a": 1}
{a: 1}

> {'a': 1}
{a: 1}

> {a: 1, b: {c: [1, true, ['hello'], {a: [1]}]}}
{a: 1, b: {c: [1, true, ['hello'], {a: [1]}]}}

> typeof({a: 1, b: {c: [1, true, ['hello'], {a: [1]}]}})
'document'