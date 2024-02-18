-- setup:
CREATE TABLE test(a INT, b TEXT, c bool);
INSERT INTO test(a, b, c) VALUES
    (1, 'foo', true),
    (1, 'bar', false),
    (1, 'bar', NULL),
    (2, 'baz', NULL),
    (2, 'baz', NULL);

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
    a: 1,
    b: "bar",
    c: null
}
{
    a: 1,
    b: "bar",
    c: false
}
{
    a: 1,
    b: "foo",
    c: true
}
{
    a: 2,
    b: "baz",
    c: null
}
*/

-- test: column
SELECT DISTINCT a FROM test;
/* result:
{
    a: 1,
}
{
    a: 2,
}
*/

-- test: column
SELECT DISTINCT a FROM test;
/* result:
{
    a: 1,
}
{
    a: 2,
}
*/

-- test: multiple columns
SELECT DISTINCT a, b FROM test;
/* result:
{
    a: 1,
    b: "bar"
}
{
    a: 1,
    b: "foo"
}
{
    a: 2,
    b: "baz"
}
*/