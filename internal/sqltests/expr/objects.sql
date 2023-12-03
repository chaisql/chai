-- test: objects.fields
> objects.fields({})
[]

> objects.fields({a: 1})
['a']

> objects.fields({a: 1, b: {c: 2}})
['a', 'b']


> objects.fields(NULL)
NULL

> objects.fields(true)
NULL

> objects.fields(false)
NULL

> objects.fields(1)
NULL

> objects.fields(1.0)
NULL

> objects.fields('hello')
NULL

> objects.fields('\xAA')
NULL

> objects.fields([])
NULL

