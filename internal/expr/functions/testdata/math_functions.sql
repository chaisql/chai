-- test: floor
> floor(2.3)
2.0
> floor(2)
2
! floor('a')
'floor(arg1) expects arg1 to be a number'

-- test: abs
> abs(NULL)
NULL
> abs(-2)
2
> abs(-2.0)
2.0
> abs('-2.0')
2.0
! abs('foo')
'cannot cast "foo" as double'
! abs(-9223372036854775808)
'integer out of range'

-- test: acos
> acos(NULL)
NULL
> acos(1)
0.0
> acos(0.5)
1.0471975511965976
> acos('0.5')
1.0471975511965976
! acos(2)
'out of range'
! acos(-2)
'out of range'
! acos(2.2)
'out of range'
! acos(-2.2)
'out of range'
! acos('foo')
'cannot cast "foo" as double'

-- test: acosh
> acosh(NULL)
NULL
> acosh(1)
0.0
> acosh(2)
1.3169578969248166
> acosh('2')
1.3169578969248166
> acosh(2.5)
1.566799236972411
! acosh(0)
'out of range'
! acosh(0.99999999)
'out of range'
! acosh('foo')
'cannot cast "foo" as double'

-- test: asin
> asin(NULL)
NULL
> asin(0)
0.0
> asin(0.5)
0.5235987755982989
! asin(2)
'out of range'
! asin(-2)
'out of range'
! asin(2.2)
'out of range'
! asin(-2.2)
'out of range'
! asin('foo')
'cannot cast "foo" as double'

-- test: asinh
> asinh(NULL)
NULL
> asinh(0)
0.0
> asinh(0.5)
0.48121182505960347
> asinh(1)
0.881373587019543
> asinh(-1)
-0.881373587019543
! asinh('foo')
'cannot cast "foo" as double'

-- test: atan
> atan(NULL)
NULL
> atan(0)
0.0
> atan(0.5)
0.4636476090008061
> atan(1)
0.7853981633974483
> atan(-1)
-0.7853981633974483
! atan('foo')
'cannot cast "foo" as double'

-- test: atan2
> atan2(NULL, NULL)
NULL
> atan2(1, NULL)
NULL
> atan2(NULL, 1)
NULL
> atan2(0, 0)
0.0
> atan2(1, 1)
0.7853981633974483
> atan2(1.1, 1.1)
0.7853981633974483
> atan2(1.1, -1.1)
2.356194490192345
! atan2('foo', 1)
'cannot cast "foo" as double'

-- test: sqrt
> sqrt(NULL)
NULL
> sqrt(4)
2.0
> sqrt(81)
9.0
> sqrt(15)
3.872983346207417
> sqrt(1.1)
1.0488088481701516
> sqrt('foo')
NULL