-- setup:
CREATE TABLE test(pk int primary key, a int, b int, c int);

CREATE INDEX test_a ON test(a);

CREATE INDEX test_b ON test(b);

INSERT INTO
    test (pk, a, b, c)
VALUES
    (1, 1, 1, 1),
    (2, 2, 2, 2),
    (3, 3, 3, 3),
    (4, 4, 4, 4),
    (5, 5, 5, 5);

-- test: non-indexed column path, ASC
EXPLAIN SELECT * FROM test ORDER BY c;
/* result:
{
    "plan": 'table.Scan("test") | rows.TempTreeSort(c)'
}
*/

-- test: non-indexed column path, DESC
EXPLAIN SELECT * FROM test ORDER BY c DESC;
/* result:
{
    "plan": 'table.Scan("test") | rows.TempTreeSortReverse(c)'
}
*/

-- test: indexed column path, ASC
EXPLAIN SELECT * FROM test ORDER BY a;
/* result:
{
    "plan": 'index.Scan("test_a")'
}
*/

-- test: indexed column path, DESC
EXPLAIN SELECT * FROM test ORDER BY a DESC;
/* result:
{
    "plan": 'index.ScanReverse("test_a")'
}
*/