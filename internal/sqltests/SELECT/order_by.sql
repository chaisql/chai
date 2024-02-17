-- setup:
CREATE TABLE test(a double, b double);
INSERT INTO test (a, b) VALUES (50, 3), (100, 4), (10, 2), (null, 1);

-- suite: no index

-- suite: with index
CREATE INDEX ON test(a);

-- test: asc
SELECT b FROM test ORDER BY a;
/* result:
{
    b: 1.0,
}
{
    b: 2.0
}
{
    b: 3.0
}
{
    b: 4.0
}
*/


-- test: asc / wildcard
SELECT * FROM test ORDER BY a;
/* result:
{
    a: null,
    b: 1.0,
}
{
    a: 10.0,
    b: 2.0
}
{
    a: 50.0,
    b: 3.0
}
{
    a: 100.0,
    b: 4.0
}
*/


-- test: desc
SELECT b FROM test ORDER BY a DESC;
/* result:
{
    b: 4.0,
}
{
    b: 3.0
}
{
    b: 2.0
}
{
    b: 1.0
}
*/

-- test: desc / wildcard
SELECT * FROM test ORDER BY a DESC;
/* result:
{
    a: 100.0,
    b: 4.0,
}
{
    a: 50.0,
    b: 3.0
}
{
    a: 10.0,
    b: 2.0
}
{
    a: null,
    b: 1.0
}
*/
