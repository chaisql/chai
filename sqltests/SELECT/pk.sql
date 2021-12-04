-- setup:
CREATE TABLE test;
INSERT INTO test(a) VALUES (1), (2), (3), (4), (5);

-- test: wildcard
SELECT pk(), a FROM test;
/* result:
{"pk()": 1, "a": 1.0}
{"pk()": 2, "a": 2.0}
{"pk()": 3, "a": 3.0}
{"pk()": 4, "a": 4.0}
{"pk()": 5, "a": 5.0}
*/
