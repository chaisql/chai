-- setup:
CREATE TABLE test(a INT PRIMARY KEY DESC, b double precision);
INSERT INTO test (a, b) VALUES (50, 2), (100, 3), (10, 1);

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
*/


-- test: asc / wildcard
SELECT * FROM test ORDER BY a;
/* result:
{
    a: 10,
    b: 1.0
}
{
    a: 50,
    b: 2.0
}
{
    a: 100,
    b: 3.0
}
*/


-- test: desc
SELECT b FROM test ORDER BY a DESC;
/* result:
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

-- test: explain desc
EXPLAIN SELECT b FROM test ORDER BY a DESC;
/* result:
{
    plan: "table.Scan(\"test\") | rows.Project(b)"
}
*/

-- test: desc / wildcard
SELECT * FROM test ORDER BY a DESC;
/* result:
{
    a: 100,
    b: 3.0
}
{
    a: 50,
    b: 2.0
}
{
    a: 10,
    b: 1.0
}
*/

-- test: explain desc / wildcard
EXPLAIN SELECT * FROM test ORDER BY a DESC;
/* result:
{
    plan: "table.Scan(\"test\")"
}
*/