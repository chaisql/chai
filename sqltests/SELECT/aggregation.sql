-- setup:
CREATE TABLE foo(a int);
INSERT INTO foo (a) VALUES (1), (2), (3), (4), (5);

-- test: GROUP BY a
SELECT a FROM foo GROUP BY a
/* result:
{"a": 1}
{"a": 2}
{"a": 3}
{"a": 4}
{"a": 5}
*/

-- test: GROUP BY a % 2
SELECT a % 2 FROM foo GROUP BY a % 2
/* result:
{"a % 2": 0}
{"a % 2": 1}
*/
