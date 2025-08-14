-- test: typeof
! typeof()

! typeof(a)
'no table specified'

> typeof(1)
'integer'

> typeof(1 + 1)
'integer'

> typeof(2.0)
'double'

> typeof(2.0)
'double'

> typeof(true)
'boolean'

> typeof('hello')
'text'

> typeof('\xAA')
'blob'

> typeof(NULL)
'null'

-- test: now
> typeof(now())
'timestamp'
