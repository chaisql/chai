-- test: typeof
! typeof()

! typeof(a)
'field not found'

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

> typeof([])
'array'

> typeof({})
'document'

> typeof(NULL)
'null'
