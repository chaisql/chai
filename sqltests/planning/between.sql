-- test: BETWEEN with index
CREATE TABLE test(pk int primary key, a int UNIQUE);
EXPLAIN SELECT * FROM test WHERE a BETWEEN 1 AND 2;
/* result:
{
    "plan": 'index.Scan("test_a_idx", [{"min": (1), "max": (2)}])'
}
*/

-- test: BETWEEN with composite index: 2 BETWEENs
CREATE TABLE test(pk int primary key, a int, b int, c int);
CREATE INDEX on test(a, b);
EXPLAIN SELECT * FROM test WHERE a BETWEEN 1 AND 2 AND b BETWEEN 3 AND 4;
/* result:
{
    "plan": 'index.Scan("test_a_b_idx", [{"min": (1), "max": (2)}]) | rows.Filter(b BETWEEN 3 AND 4)'
}
*/

-- test: BETWEEN with composite index: one BETWEEN at the end
CREATE TABLE test(pk int primary key, a int, b int, c int, d int, e int);
CREATE INDEX on test(a, b, c, d);
EXPLAIN SELECT * FROM test WHERE a = 1 AND b = 10 AND c = 100 AND d BETWEEN 1000 AND 2000 AND e > 10000;
/* result:
{
    "plan": 'index.Scan("test_a_b_c_d_idx", [{"min": (1, 10, 100, 1000), "max": (1, 10, 100, 2000)}]) | rows.Filter(e > 10000)'
}
*/
