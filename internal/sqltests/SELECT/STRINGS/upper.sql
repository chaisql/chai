-- setup:
CREATE TABLE test(
    a TEXT,
    b INT,
    c BOOL,
    d DOUBLE
);

INSERT INTO test (a, b, c, d) VALUES (
    "foo",
    42,
    true,
    42.42,
);

-- test: TEXT value
SELECT UPPER(a) FROM test;
/* result:
{
    "UPPER(a)": "FOO" 
}
*/


-- test: INT value
SELECT UPPER(b) FROM test;
/* result:
{
    "UPPER(b)": NULL 
}
*/


-- test: BOOL value
SELECT UPPER(c) FROM test;
/* result:
{
    "UPPER(c)": NULL 
}
*/

-- test: DOUBLE value
SELECT UPPER(d) FROM test;
/* result:
{
    "UPPER(d)": NULL 
}
*/

-- test: cast INT
SELECT UPPER(CAST(b as TEXT)) FROM test;
/* result:
{
    "UPPER(CAST(b AS text))": "42" 
}
*/

-- test: cast BOOL
SELECT UPPER(CAST(c as TEXT)) FROM test;
/* result:
{
    "UPPER(CAST(c AS text))": "TRUE" 
}
*/

-- test: cast DOUBLE
SELECT UPPER(CAST(d as TEXT)) FROM test;
/* result:
{
    "UPPER(CAST(d AS text))": "42.42" 
}
*/

