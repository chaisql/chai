-- setup:
CREATE TABLE test(a double precision primary key, b int, c bool);
INSERT INTO test(a, b, c) VALUES (1, 1, true);

-- suite: no index

-- suite: with index
CREATE INDEX ON test(a);

-- test: wildcard
SELECT * FROM test;
/* result:
{"a": 1.0, "b": 1, "c": true}
*/

-- test: multiple wildcards
SELECT *, * FROM test;
/* result:
{
    "a": 1.0,
    "b": 1,
    "c": true,
    "a": 1.0,
    "b": 1,
    "c": true
}
*/

-- test: column paths
SELECT a, b, c FROM test;
/* result:
{
    "a": 1.0,
    "b": 1,
    "c": true
}
*/

-- test: column path, wildcards and expressions
SELECT a AS A, b + 1, * FROM test;
/* result:
{
    "A": 1.0,
    "b + 1": 2,
    "a": 1.0,
    "b": 1,
    "c": true
}
*/

-- test: wildcard and other column
SELECT *, c FROM test;
/* result:
{
    "a": 1.0,
    "b": 1,
    "c": true,
    "c": true
}
*/
