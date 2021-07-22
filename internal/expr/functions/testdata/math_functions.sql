-- test: math.floor
> math.floor(2.3)
2.0
> math.floor(2)
2
! math.floor('a')
'floor(arg1) expects arg1 to be a number'

-- test: math.abs
> math.abs(NULL)
NULL
> math.abs(-2)
2
> math.abs(-2.0)
2.0
> math.abs('-2.0')
2.0
! math.abs('foo')
'cannot cast "foo" as double'

-- test: math.acos
> math.acos(NULL)
NULL
> math.acos(1)
0.0
> math.acos(0.5)
1.0471975511965976
> math.acos('0.5')
1.0471975511965976
! math.acos(2)
'out of range'
! math.acos(-2)
'out of range'
! math.acos(2.2)
'out of range'
! math.acos(-2.2)
'out of range'
! math.acos('foo')
'cannot cast "foo" as double'

-- test: math.acosh
> math.acosh(NULL)
NULL
> math.acosh(1)
0.0
> math.acosh(2)
1.3169578969248166
> math.acosh('2')
1.3169578969248166
> math.acosh(2.5)
1.566799236972411
! math.acosh(0)
'out of range'
! math.acosh(0.99999999)
'out of range'
! math.acosh('foo')
'cannot cast "foo" as double'

-- test: math.asin
> math.asin(NULL)
NULL
> math.asin(0)
0.0
> math.asin(0.5)
0.5235987755982989
! math.asin(2)
'out of range'
! math.asin(-2)
'out of range'
! math.asin(2.2)
'out of range'
! math.asin(-2.2)
'out of range'
! math.asin('foo')
'cannot cast "foo" as double'

-- test: math.asinh
> math.asinh(NULL)
NULL
> math.asinh(0)
0.0
> math.asinh(0.5)
0.48121182505960347
> math.asinh(1)
0.881373587019543
> math.asinh(-1)
-0.881373587019543
! math.asinh('foo')
'cannot cast "foo" as double'

-- test: math.atan
> math.atan(NULL)
NULL
> math.atan(0)
0.0
> math.atan(0.5)
0.4636476090008061
> math.atan(1)
0.7853981633974483
> math.atan(-1)
-0.7853981633974483
! math.atan('foo')
'cannot cast "foo" as double'

-- test: math.atan2
> math.atan2(NULL, NULL)
NULL
> math.atan2(1, NULL)
NULL
> math.atan2(NULL, 1)
NULL
> math.atan2(0, 0)
0.0
> math.atan2(1, 1)
0.7853981633974483
> math.atan2(1.1, 1.1)
0.7853981633974483
> math.atan2(1.1, -1.1)
2.356194490192345
! math.atan2('foo', 1)
'cannot cast "foo" as double'
