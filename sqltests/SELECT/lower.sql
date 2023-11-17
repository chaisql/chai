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
SELECT LOWER(a) FROM test;
/* result:
{
    "LOWER(a)": "foo" 
}
*/


-- test: INT value
SELECT LOWER(b) FROM test;
/* result:
{
    "LOWER(b)": NULL 
}
*/


-- test: BOOL value
SELECT LOWER(c) FROM test;
/* result:
{
    "LOWER(c)": NULL 
}
*/

-- test: DOUBLE value
SELECT LOWER(d) FROM test;
/* result:
{
    "LOWER(d)": NULL 
}
*/

-- test: ARRAY value
SELECT LOWER(e) FROM test;
/* result:
{
    "LOWER(e)": NULL 
}
*/

-- test: DOCUMENT value
SELECT LOWER(f) FROM test;
/* result:
{
    "LOWER(f)": NULL 
}
*/

-- test: cast INT
SELECT LOWER(CAST(b as TEXT)) FROM test;
/* result:
{
    "LOWER(CAST(b AS text))": "42" 
}
*/

-- test: cast BOOL
SELECT LOWER(CAST(c as TEXT)) FROM test;
/* result:
{
    "LOWER(CAST(c AS text))": "true" 
}
*/

-- test: cast DOUBLE
SELECT LOWER(CAST(d as TEXT)) FROM test;
/* result:
{
    "LOWER(CAST(d AS text))": "42.42" 
}
*/

-- test: cast ARRAY
SELECT LOWER(CAST(e as TEXT)) FROM test;
/* result:
{
    "LOWER(CAST(e AS text))": "[\"a\", \"b\", \"c\", \"d\", \"e\"]" 
}
*/

-- test: cast DOCUMENT
SELECT LOWER(CAST(f as TEXT)) FROM test;
/* result:
{
    "LOWER(CAST(f AS text))": "{\"a\": \"hello\", \"b\": \"world\"}" 
}
*/
