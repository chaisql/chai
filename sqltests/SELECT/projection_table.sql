-- setup:
CREATE TABLE foo;
INSERT INTO foo(a, b, c) VALUES (1, {a: 1}, [true]);

-- test: wildcard
SELECT * FROM foo;
/* result:
{"a": 1.0, "b": {"a": 1.0}, "c": [true]}
*/

-- test: multiple wildcards
SELECT *, * FROM foo;
-- error:

-- test: field paths
SELECT a, b, c FROM foo;
/* result:
{
    "a": 1.0,
    "b": {"a": 1.0},
    "c": [true]
}
*/

-- test: field path, wildcards and expressions
SELECT a AS A, b.a + 1, * FROM foo;
/* result:
{
    "A": 1.0,
    "b.a + 1": 2.0,
    "a": 1.0,
    "b": {"a": 1.0},
    "c": [true]
}
*/