-- setup:
CREATE TABLE test(a INT, b DOUBLE);
CREATE INDEX on test(a DESC, b DESC);
INSERT INTO test (a, b) VALUES (50, 2), (100, 3), (10, 1), (100, 4);

-- test: asc
SELECT a, b FROM test ORDER BY a;
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
{
    a: 100,
    b: 4.0
}
*/

-- test: asc / explain
EXPLAIN SELECT a FROM test ORDER BY a;
/* result:
{
    plan: "index.ScanReverse(\"test_a_b_idx\") | docs.Project(a)"
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
{
    a: 100,
    b: 4.0
}
*/

-- test: desc / no index
SELECT a, b FROM test ORDER BY b DESC;
/* result:
{
    a: 100,
    b: 4.0
}
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

-- test: desc / no index: explain
EXPLAIN SELECT a, b FROM test ORDER BY b DESC;
/* result:
{
    plan: "table.Scan(\"test\") | docs.Project(a, b) | docs.TempTreeSortReverse(b)"
}
*/

-- test: desc / with index
SELECT a, b FROM test ORDER BY a DESC;
/* result:
{
    a: 100,
    b: 4.0
}
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

-- test: desc / with index: explain
EXPLAIN SELECT a, b FROM test ORDER BY a DESC;
/* result:
{
    plan: "index.Scan(\"test_a_b_idx\") | docs.Project(a, b)"
}
*/

-- test: desc / with index / multi field
SELECT a, b FROM test WHERE a = 100 ORDER BY b DESC;
/* result:
{
    a: 100,
    b: 4.0
}
{
    a: 100,
    b: 3.0
}
*/

-- test: explain desc / with index / multi field
EXPLAIN SELECT a, b FROM test WHERE a = 100 ORDER BY b DESC;
/* result:
{
    plan: "index.Scan(\"test_a_b_idx\", [{\"min\": [100], \"exact\": true}]) | docs.Project(a, b)"
}
*/

-- test: desc / wildcard
SELECT * FROM test ORDER BY a DESC;
/* result:
{
    a: 100,
    b: 4.0
}
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
