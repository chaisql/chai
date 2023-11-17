-- setup:
CREATE TABLE test(
    a TEXT,
    b INT,
    c BOOL,
    d DOUBLE,
    e ARRAY,
    f (
        ...
    )
);

INSERT INTO test (a, b, c, d, e, f) VALUES (
    "foo",
    42,
    true,
    42.42,
    ["A", "b", "C", "d", "E"],
    {
        a: "hello",
        b: "WorlD"
    }
);

-- test: TEXT value
SELECT strings.UPPER(a) FROM test;
/* result:
{
    "UPPER(a)": "FOO" 
}
*/


-- test: INT value
SELECT strings.UPPER(b) FROM test;
/* result:
{
    "UPPER(b)": NULL 
}
*/


-- test: BOOL value
SELECT strings.UPPER(c) FROM test;
/* result:
{
    "UPPER(c)": NULL 
}
*/

-- test: DOUBLE value
SELECT strings.UPPER(d) FROM test;
/* result:
{
    "UPPER(d)": NULL 
}
*/

-- test: ARRAY value
SELECT strings.UPPER(e) FROM test;
/* result:
{
    "UPPER(e)": NULL 
}
*/

-- test: DOCUMENT value
SELECT strings.UPPER(f) FROM test;
/* result:
{
    "UPPER(f)": NULL 
}
*/

-- test: cast INT
SELECT strings.UPPER(CAST(b as TEXT)) FROM test;
/* result:
{
    "UPPER(CAST(b AS text))": "42" 
}
*/

-- test: cast BOOL
SELECT strings.UPPER(CAST(c as TEXT)) FROM test;
/* result:
{
    "UPPER(CAST(c AS text))": "TRUE" 
}
*/

-- test: cast DOUBLE
SELECT strings.UPPER(CAST(d as TEXT)) FROM test;
/* result:
{
    "UPPER(CAST(d AS text))": "42.42" 
}
*/

-- test: cast ARRAY
SELECT strings.UPPER(CAST(e as TEXT)) FROM test;
/* result:
{
    "UPPER(CAST(e AS text))": "[\"A\", \"B\", \"C\", \"D\", \"E\"]" 
}
*/

-- test: cast DOCUMENT
SELECT strings.UPPER(CAST(f as TEXT)) FROM test;
/* result:
{
    "UPPER(CAST(f AS text))": "{\"A\": \"HELLO\", \"B\": \"WORLD\"}" 
}
*/
