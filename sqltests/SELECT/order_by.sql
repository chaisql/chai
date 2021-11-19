-- setup:
CREATE TABLE test;
INSERT INTO test (a) VALUES (1), (2), (3);

-- test: asc
SELECT *, a FROM test ORDER BY a;
/* result:
{
    a: 1.0,
    a: 1.0
}
{
    a: 2.0,
    a: 2.0
}
{
    a: 3.0,
    a: 3.0
}
*/

-- test: desc
SELECT *, a FROM test ORDER BY a DESC;
/* result:
{
    a: 3.0,
    a: 3.0
}
{
    a: 2.0,
    a: 2.0
}
{
    a: 1.0,
    a: 1.0
}
*/

-- test: 
SELECT * FROM test ORDER BY a DESC;
/* result:
{
    a: 3.0
}
{
    a: 2.0
}
{
    a: 1.0
}
*/