-- setup:
CREATE TABLE test(a int, b int, c int, PRIMARY KEY (a, b));

INSERT INTO
    test (a, b, c)
VALUES
    (1, 1, 1),
    (2, 2, 2),
    (3, 3, 3),
    (4, 4, 4),
    (5, 5, 5);

-- test: =
EXPLAIN SELECT * FROM test WHERE a = 10 AND b = 5;
/* result:
{
    "plan": 'table.Scan("test", [{\"min\": [10, 5], \"exact\": true}])'
}
*/

-- test: > vs =
EXPLAIN SELECT * FROM test WHERE a > 10 AND b = 5;
/* result:
{
    "plan": 'table.Scan(\"test\", [{"min": [10], "exclusive": true}]) | docs.Filter(b = 5)'
}
*/

-- test: >
EXPLAIN SELECT * FROM test WHERE a > 10 AND b > 5;
/* result:
{
    "plan": 'table.Scan("test", [{"min": [10], "exclusive": true}]) | docs.Filter(b > 5)'
}
*/

-- test: >=
EXPLAIN SELECT * FROM test WHERE a >= 10 AND b > 5;
/* result:
{
    "plan": 'table.Scan("test", [{"min": [10]}]) | docs.Filter(b > 5)'
}
*/

-- test: <
EXPLAIN SELECT * FROM test WHERE a < 10 AND b > 5;
/* result:
{
    "plan": 'table.Scan("test", [{"max": [10], "exclusive": true}]) | docs.Filter(b > 5)'
}
*/
