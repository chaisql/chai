-- setup:
CREATE TABLE test(pk INT PRIMARY KEY, a int);
INSERT INTO test (pk, a) VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5);

-- test: GROUP BY a
SELECT a FROM test GROUP BY a
/* result:
{"a": 1}
{"a": 2}
{"a": 3}
{"a": 4}
{"a": 5}
*/

-- test: GROUP BY a % 2
SELECT a % 2 FROM test GROUP BY a % 2
/* result:
{"a % 2": 0}
{"a % 2": 1}
*/
