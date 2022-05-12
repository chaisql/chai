-- setup:
CREATE TABLE test;
INSERT INTO test (a) VALUES (1), (2), (3);

-- suite: no index

-- suite: with index
CREATE INDEX ON test(a);

-- test: asc
SELECT * FROM test ORDER BY a;
/* result:
{
    a: 1.0
}
{
    a: 2.0
}
{
    a: 3.0
}
*/

-- test: desc
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

