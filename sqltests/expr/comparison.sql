-- test: boolean with boolean
> true < true
false

> true < true
false

> true = true
true

> true >= true
true

> true <= true
true

> false < false
false

> true > false
true

> false > true
false

> false > false
false

> true != false
true

> false != false
false

> true >= false
true

> false <= true
true

> false >= true
false

-- test: boolean with integer (incompatible types)
! true > 1
'cannot compare boolean with integer'

! true BETWEEN 1 AND 2
'cannot compare boolean with integer'

-- test: boolean with text (incompatible types)
! true > 'a'
'cannot cast "a" as boolean'

! true < 'a'
'cannot cast "a" as boolean'

! true BETWEEN 'a' AND 'b'
'cannot cast "a" as boolean'

-- test: boolean with boolean
> true > true
false

> true < true
false

> true = true
true

> true >= true
true

> true <= true
true

> true > false
true

> false > true
false

> false > false
false

> true != true
false

> true != false
true

> false != false
false

-- test: boolean: additional <= and >= cases
> true >= false
true

> false >= true
false

> true <= false
false

! true > 1
'cannot compare boolean with integer'

! true < 1
'cannot compare boolean with integer'

! true = 1
'cannot compare boolean with integer'

! true BETWEEN 1 AND 2
'cannot compare boolean with integer'

-- test: boolean compared with text value
> true > 't'
false

> true < 't'
false

> true = 't'
true

-- test: boolean with NULL (three-valued logic)
> true > NULL
NULL

> true < NULL
NULL

> true != NULL
NULL

> NULL != NULL
NULL

-- test: integer comparisons (Postgres-like behavior)
-- simple operators
> 1 > 0
true

> 2 = 2
true

> -1 < 0
true

> -1 <= -1
true

> 0 >= 0
true

> 5 != 3
true

> 3 != 3
false

> 0 != 0
false

-- additional boundary comparisons
> 1 >= 1
true

> 1 <= 1
true

> -2 >= -3
true

-- comparisons mixing sign and zero
> 0 > -1
true

> -5 < -3
true

-- large integers and limits
> 2147483647 = 2147483647
true

> -2147483648 = -2147483648
true

> 9223372036854775807 = 9223372036854775807
true

> -9223372036854775808 = -9223372036854775808
true

-- BETWEEN for integers
> 5 BETWEEN 1 AND 10
true

> 1 BETWEEN 1 AND 5
true

> 10 BETWEEN 1 AND 9
false

> 5 BETWEEN NULL AND 10
NULL

-- integer NULL handling
> 1 > NULL
NULL

> NULL < 1
NULL

> NULL = NULL
NULL

> 1 != NULL
NULL

-- mixed-type: integer with double
> 1 = 1.0
true

> 1.5 > 1
true

> 2 = 2.0
true

-- mixed-type: integer with text
-- The project expects numeric-like text to compare equal to integers
> 1 = '1'
true

> 1 = '01'
true

-- Non-numeric text should error when coerced to integer
! 1 = 'a'
'cannot cast "a" as integer: strconv.ParseInt: parsing "a": invalid syntax'

! 1 > 'a'
'cannot cast "a" as integer: strconv.ParseInt: parsing "a": invalid syntax'

-- mixed-type: integer with boolean
-- In Postgres this raises an operator error
! 1 = true
'cannot compare integer with boolean'

! 1 > true
'cannot compare integer with boolean'

-- mixed numeric comparisons across sizes
> 100000000000 = 100000000000
true

> 100000000000 > 99999999999
true

-- final sanity checks
> -9223372036854775808 < 0
true

> 9223372036854775807 > 0
true

> true = 't'
true

-- test: boolean with null (three-valued logic)
> true > NULL
NULL

> true < NULL
NULL

> true = NULL
NULL

> true BETWEEN NULL AND NULL
NULL

-- NULL handling for not-equal
> true != NULL
NULL

> NULL != NULL
NULL

-- test: integer comparisons (Postgres-like behavior)
> 1 > 0
true

> 0 < 1
true

> 2 = 2
true

> -1 < 0
true

> -1 <= -1
true

> 0 >= 0
true

> 5 != 3
true

> 3 != 3
false

> 0 != 0
false

-- comparisons mixing sign and zero
> 0 > -1
true

> -5 < -3
true

-- comparisons with large integers and boundaries
> 2147483647 = 2147483647
true

> -2147483648 = -2147483648
true

> 9223372036854775807 = 9223372036854775807
true

> -9223372036854775808 = -9223372036854775808
true

-- BETWEEN for integers
> 5 BETWEEN 1 AND 10
true

> 1 BETWEEN 1 AND 5
true

> 10 BETWEEN 1 AND 9
false

> 5 BETWEEN NULL AND 10
NULL

-- mixed-type: integer with double (numeric comparison)
> 1 = 1.0
true

> 1.5 > 1
true

> 2 = 2.0
true

-- mixed-type: integer with text
-- In Postgres a string literal without explicit type can be coerced to numeric when comparing with integer
> 1 = '1'
true

> 1 = '01'
true

-- non-numeric text causes an error when trying to coerce to integer
! 1 = 'a'
'cannot cast "a" as integer: strconv.ParseInt: parsing "a": invalid syntax'

! 1 > 'a'
'cannot cast "a" as integer: strconv.ParseInt: parsing "a": invalid syntax'

-- mixed-type: integer with boolean
-- Postgres: comparing integer and boolean is not supported (operator does not exist)
! 1 = true
'cannot compare integer with boolean'

! 1 > true
'cannot compare integer with boolean'

-- integer with NULL (three-valued logic)
> 1 > NULL
NULL

> NULL < 1
NULL

> NULL = NULL
NULL

> 2 = 2
true

> -1 < 0
true

> -1 <= -1
true

> 0 >= 0
true

> 5 != 3
true

> 3 != 3
false

> 0 != 0
false

-- comparisons mixing sign and zero
> 0 > -1
true

> -5 < -3
true

-- test: comparisons with large integers and boundaries
> 2147483647 = 2147483647
true

> -2147483648 < 0
true

> 2147483648 = 2147483648
true

> 9223372036854775807 > 0
true

-- test: BETWEEN for integers
> 5 BETWEEN 1 AND 10
true

> 1 BETWEEN 1 AND 5
true

> 10 BETWEEN 1 AND 9
false

> 5 BETWEEN NULL AND 10
NULL

-- test: mixed-type: integer with double (numeric comparison)
> 1 = 1.0
true

> 1.5 > 1
true

> 2 = 2.0
true

-- test: mixed-type: integer with text
-- test: In Postgres a numeric string literal can be coerced, so '1' compares equal to 1
> 1 = '1'
true

> 1 = '01'
true

-- test: non-numeric text causes an error when trying to coerce to integer
! 1 = 'a'
'cannot cast "a" as integer: strconv.ParseInt: parsing "a": invalid syntax'

! 1 > 'a'
'cannot cast "a" as integer: strconv.ParseInt: parsing "a": invalid syntax'

-- test: mixed-type: integer with boolean
-- Postgres: comparing integer and boolean is not supported (operator does not exist)
! 1 = true
'cannot compare integer with boolean'

! 1 > true
'cannot compare integer with boolean'

-- test: integer with NULL (three-valued logic)
> 1 > NULL
NULL

> NULL < 1
NULL

> NULL = NULL
NULL

-- test: boolean with null (three-valued logic)
> true > NULL
NULL

> true < NULL
NULL

> true = NULL
NULL

> true BETWEEN NULL AND NULL
NULL

-- test: NULL handling for not-equal
> true != NULL
NULL

> NULL != NULL
NULL

-- test: integer comparisons
> 1 > 0
true

> 0 < 1
true

> 2 = 2
true

> -1 < 0
true

> -1 <= -1
true

> 0 >= 0
true

> 5 != 3
true

> 3 != 3
false

> 0 != 0
false

-- test: comparisons mixing sign and zero
> 0 > -1
true

> -5 < -3
true

-- test: comparisons with large integers
> 100000000000 > 99999999999
true

> 100000000000 = 100000000000
true

-- test: mixed-type: integer with double (numeric comparison)
> 1 = 1.0
true

> 1.5 > 1
true

> 2 = 2.0
true

-- test: mixed-type: integer with text
> 1 = '1'
true

! 1 > 'a'
'cannot cast "a" as integer: strconv.ParseInt: parsing "a": invalid syntax'

-- test: mixed-type: integer with boolean -> typically NULL
! 1 = true
'cannot compare integer with bool'

-- test: integer with NULL
> 1 > NULL
NULL

> NULL < 1
NULL

> NULL = NULL
NULL
