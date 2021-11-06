-- setup:
CREATE TABLE test;
INSERT INTO test(a, b, c) VALUES (1, {a: 1}, [true]);

-- suite: no index

-- suite: with index
CREATE INDEX ON test(a);

-- test: wildcard
SELECT * FROM test;
/* result:
{"a": 1.0, "b": {"a": 1.0}, "c": [true]}
*/

-- test: multiple wildcards
SELECT *, * FROM test;
-- error:

-- test: field paths
SELECT a, b, c FROM test;
/* result:
{
    "a": 1.0,
    "b": {"a": 1.0},
    "c": [true]
}
*/

-- test: field path, wildcards and expressions
SELECT a AS A, b.a + 1, * FROM test;
/* result:
{
    "A": 1.0,
    "b.a + 1": 2.0,
    "a": 1.0,
    "b": {"a": 1.0},
    "c": [true]
}
*/

-- test: wildcard and other field
SELECT *, c FROM test;
-- error:
