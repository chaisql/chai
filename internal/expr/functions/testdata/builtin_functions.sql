-- test: typeof
! typeof()

! typeof(a)
'no table specified'

> typeof(1)
'integer'

> typeof(1 + 1)
'integer'

> typeof(2.0)
'double precision'

> typeof(2.0)
'double precision'

> typeof(true)
'boolean'

> typeof('hello')
'text'

> typeof('\xAA')
'bytea'

> typeof(NULL)
'null'

-- test: now
> typeof(now())
'timestamp'
