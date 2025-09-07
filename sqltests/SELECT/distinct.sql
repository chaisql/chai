-- setup:
CREATE TABLE test(pk INT PRIMARY KEY, a INT, b TEXT, c bool);
INSERT INTO test(pk, a, b, c) VALUES
    (1, 1, 'foo', true),
    (2, 1, 'bar', false),
    (3, 1, 'bar', NULL),
    (4, 2, 'baz', NULL),
    (5, 2, 'baz', NULL);

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
    pk: 1,
    a: 1,
    b: "foo",
    c: true
}
{
    pk: 2,
    a: 1,
    b: "bar",
    c: false
}
{
    pk: 3,
    a: 1,
    b: "bar",
    c: null
}
{
    pk: 4,
    a: 2,
    b: "baz",
    c: null
}
{
    pk: 5,
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