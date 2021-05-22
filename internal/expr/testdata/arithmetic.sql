-- test: basic arithmetic
> 1+1
2

> 1 + NULL
NULL

> 1 - 1
0

> 1 - NULL
NULL

> 1 * 1
1

> 1 * NULL
NULL

> 1 / 1
1

> 1 / NULL
NULL

> 4 % 3
1

> 1 % NULL
NULL

> 1 & 1
1

> 1 & NULL
NUll

> 1 | 1
1

> 1 | NULL
NULL

> 1 ^ 1
0

> 1 ^ NULL
NULL

-- test: divide by zero
> 1 / 0
NULL

-- test: arithmetic with unexisting field
! 1 + a
'field not found'

! 1 - a
'field not found'

! 1 * a
'field not found'

! 1 / a
'field not found'

! 1 % a
'field not found'

! 1 & a
'field not found'

! 1 | a
'field not found'

! 1 ^ a
'field not found'
