-- test: simple case
> COALESCE(1,2,3)
1

-- test: with null
> COALESCE(null,2,3)
2

-- test: with different values type
> COALESCE('hey',2,3)
'hey'

-- test: with more than one null value with integer
> COALESCE(null, null, null,3)
3

-- test: with more than one null value with text
> COALESCE(null, null, null, 'hey')
'hey'
