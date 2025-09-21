-- setup:
CREATE TABLE test(
    pk INT PRIMARY KEY, 
    a TEXT,
    b INT,
    c BOOL,
    d DOUBLE PRECISION
);

INSERT INTO test (pk, a, b, c, d) VALUES (
    1,
    'FOO',
    42,
    true,
    42.42
);

-- test: TEXT value
SELECT LOWER(a) FROM test;
/* result:
{
    "LOWER(a)": 'foo' 
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

-- test: DOUBLE PRECISION value
SELECT LOWER(d) FROM test;
/* result:
{
    "LOWER(d)": NULL 
}
*/

-- test: cast INT
SELECT LOWER(CAST(b as TEXT)) FROM test;
/* result:
{
    "LOWER(CAST(b AS text))": '42' 
}
*/

-- test: cast BOOL
SELECT LOWER(CAST(c as TEXT)) FROM test;
/* result:
{
    "LOWER(CAST(c AS text))": 'true' 
}
*/

-- test: cast DOUBLE
SELECT LOWER(CAST(d as TEXT)) FROM test;
/* result:
{
    "LOWER(CAST(d AS text))": '42.42' 
}
*/
