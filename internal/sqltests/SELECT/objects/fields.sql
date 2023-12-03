-- setup:
CREATE TABLE test(
    a TEXT,
    b INT,
    c BOOL,
    d DOUBLE,
    e ARRAY,
    f OBJECT
);

INSERT INTO test (a, b, c, d, e, f) VALUES (
    "FOO",
    42,
    true,
    42.42,
    ["A", "b", "C", "d", "E"],
    {
        a: "HELLO",
        b: "WorlD"
    }
);

-- test: TEXT value
SELECT objects.fields(a) FROM test;
/* result:
{
    "objects.fields(a)": NULL
}
*/

-- test: INT value
SELECT objects.fields(b) FROM test;
/* result:
{
    "objects.fields(b)": NULL
}
*/


-- test: BOOL value
SELECT objects.fields(c) FROM test;
/* result:
{
    "objects.fields(c)": NULL
}
*/

-- test: DOUBLE value
SELECT objects.fields(d) FROM test;
/* result:
{
    "objects.fields(d)": NULL
}
*/

-- test: ARRAY value
SELECT objects.fields(e) FROM test;
/* result:
{
    "objects.fields(e)": NULL
}
*/

-- test: OBJECT value
SELECT objects.fields(f) FROM test;
/* result:
{
    "objects.fields(f)": ["a", "b"]
}
*/

-- test: wildcard
SELECT objects.fields(*) FROM test;
/* result:
{
    "objects.fields(*)": ["a", "b", "c", "d", "e", "f"]
}
*/