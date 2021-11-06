-- setup:
CREATE TABLE test;
INSERT INTO test(a, b, c) VALUES
    (1, {d: 1}, [true]),
    (1, {d: 2}, [false]),
    (1, {d: 2}, []),
    (2, {d: 3}, []),
    (2, {d: 3}, []),
    ([true], 1, 1.5);

-- test: literal
SELECT DISTINCT 'a' FROM test;
/* result:
{
    `"a"`: "a",
}
*/

-- test: wildcard
SELECT DISTINCT * FROM test;
/* result:
{
    a: 1.0,
    b: {d: 1.0},
    c: [true]
}
{
    a: 1.0,
    b: {d: 2.0},
    c: []
}
{
    a: 1.0,
    b: {d: 2.0},
    c: [false]
}
{
    a: 2.0,
    b: {d: 3.0},
    c: []
}
{
    a: [true],
    b: 1.0,
    c: 1.5
}
*/

-- test: field path
SELECT DISTINCT a FROM test;
/* result:
{
    a: 1.0,
}
{
    a: 2.0,
}
{
    a: [true],
}
*/

-- test: field path
SELECT DISTINCT a FROM test;
/* result:
{
    a: 1.0,
}
{
    a: 2.0,
}
{
    a: [true],
}
*/

-- test: multiple field paths
SELECT DISTINCT a, b.d FROM test;
/* result:
{
    a: 1.0,
    "b.d": 1.0
}
{
    a: 1.0,
    "b.d": 2.0
}
{
    a: 2.0,
    "b.d": 3.0
}
{
    a: [true],
    "b.d": NULL
}
*/