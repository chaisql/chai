-- setup:
CREATE TABLE test(a int, b int, c int);
CREATE INDEX test_a ON test(a);
CREATE INDEX test_b ON test(b);
INSERT INTO test (a, b, c) VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, 5);

-- test: =
EXPLAIN SELECT * FROM test WHERE a = 10 AND b = 5;
/* result:
{
    "plan": 'indexScan("test_a", 10) | filter(b = 5)'
}
*/

-- test: > vs =
EXPLAIN SELECT * FROM test WHERE a > 10 AND b = 5;
/* result:
{
    "plan": 'indexScan("test_b", 5) | filter(a > 10)'
}
*/

-- test: >
EXPLAIN SELECT * FROM test WHERE a > 10 AND b > 5;
/* result:
{
    "plan": 'indexScan("test_a", [10, -1, true]) | filter(b > 5)'
}
*/

-- test: >=
EXPLAIN SELECT * FROM test WHERE a >= 10 AND b > 5;
/* result:
{
    "plan": 'indexScan("test_a", [10, -1]) | filter(b > 5)'
}
*/
-- test: <
EXPLAIN SELECT * FROM test WHERE a < 10 AND b > 5;
/* result:
{
    "plan": 'indexScan("test_a", [-1, 10, true]) | filter(b > 5)'
}
*/
