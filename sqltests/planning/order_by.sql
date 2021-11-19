-- setup:
CREATE TABLE test(a int, b int, c int);
CREATE INDEX test_a ON test(a);
CREATE INDEX test_b ON test(b);
INSERT INTO test (a, b, c) VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, 5);

-- test: non-indexed field path, ASC
EXPLAIN SELECT * FROM test ORDER BY c;
/* result:
{
    "plan": 'seqScan(test) | tempTreeSort(c)'
}
*/

-- test: non-indexed field path, DESC
EXPLAIN SELECT * FROM test ORDER BY c DESC;
/* result:
{
    "plan": 'seqScan(test) | tempTreeSortReverse(c)'
}
*/

