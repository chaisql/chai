-- setup:
CREATE TABLE test(a int, b int, c int);

CREATE INDEX test_a ON test(a);

CREATE INDEX test_b ON test(b);

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
    "plan": 'index.Scan("test_a", [{"min": [10], "exact": true}]) | rows.Filter(b = 5)'
}
*/

-- test: > vs =
EXPLAIN SELECT * FROM test WHERE a > 10 AND b = 5;
/* result:
 {
    "plan": 'index.Scan("test_b", [{"min": [5], "exact": true}]) | rows.Filter(a > 10)'
 }
*/

-- test: >
EXPLAIN SELECT * FROM test WHERE a > 10 AND b > 5;
/* result:
 {
    "plan": 'index.Scan("test_a", [{"min": [10], "exclusive": true}]) | rows.Filter(b > 5)'
 }
*/

-- test: >=
EXPLAIN SELECT * FROM test WHERE a >= 10 AND b > 5;
/* result:
 {
    "plan": 'index.Scan("test_a", [{"min": [10]}]) | rows.Filter(b > 5)'
 }
*/

-- test: <
EXPLAIN SELECT * FROM test WHERE a < 10 AND b > 5;
/* result:
 {
    "plan": 'index.Scan("test_a", [{"max": [10], "exclusive": true}]) | rows.Filter(b > 5)'
 }
*/

-- test: BETWEEN
EXPLAIN SELECT * FROM test WHERE a BETWEEN 4 AND 5 AND b > 5;
/* result:
 {
    "plan": 'index.Scan("test_a", [{"min": [4], "max": [5]}]) | rows.Filter(b > 5)'
 }
*/

-- test: with two paths
EXPLAIN SELECT * FROM test WHERE a < b + 1;
/* result:
 {
    "plan": 'table.Scan("test") | rows.Filter(a < b + 1)'
 }
*/

 -- test: with two paths, other side
EXPLAIN SELECT * FROM test WHERE a + 1 < b;
/* result:
 {
    "plan": 'table.Scan("test") | rows.Filter(a + 1 < b)'
 }
*/

  -- test: with two paths, with IN
EXPLAIN SELECT * FROM test WHERE a IN (1, b + 3);
/* result:
 {
    "plan": 'table.Scan("test") | rows.Filter(a IN [1, b + 3])'
 }
*/