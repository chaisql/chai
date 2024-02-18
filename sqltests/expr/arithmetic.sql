-- test: basic arithmetic
> 1+1
2

> 1 + NULL
NULL

> 1-1
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
! 1 / 0
'division by zero'

-- test: arithmetic with unexisting column
! 1 + a
'no table specified'

! 1 - a
'no table specified'

! 1 * a
'no table specified'

! 1 / a
'no table specified'

! 1 % a
'no table specified'

! 1 & a
'no table specified'

! 1 | a
'no table specified'

! 1 ^ a
'no table specified'

-- test: division
> 1 / 2
0

> 1 + 1 * 2 / 4
1

-- test: arithmetic with different types
> 1 + 1.5
2.5

> 1 + '2'
NULL

> 1 + true
NULL

> 4.5 + 4.5
9.0

! 1000000000 * 1000000000

! 1000000000000000000 * 1000000000000000000 * 1000000000000000000
