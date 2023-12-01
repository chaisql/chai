-- test: VALUES, with all fields
CREATE TABLE test (a TEXT, b TEXT, c TEXT);
INSERT INTO test (a, b, c) VALUES ('a', 'b', 'c');
SELECT pk(), * FROM test;
/* result:
{
  "pk()": [1],
  "a": "a",
  "b": "b",
  "c": "c"
}
*/

-- test: VALUES, with a few fields
CREATE TABLE test (a TEXT, b TEXT, c TEXT);
INSERT INTO test (b, a) VALUES ('b', 'a');
SELECT pk(), * FROM test;
/* result:
{
  "pk()": [1],
  "a": "a",
  "b": "b"
}
*/

-- test: VALUES, with too many fields
CREATE TABLE test (a TEXT, b TEXT, c TEXT);
INSERT INTO test (b, a, c, d) VALUES ('b', 'a', 'c', 'd');
-- error: table has no field d

-- test: variadic, VALUES, with all fields
CREATE TABLE test (a TEXT, b TEXT, c TEXT, ...);
INSERT INTO test (a, b, c) VALUES ('a', 'b', 'c');
SELECT pk(), * FROM test;
/* result:
{
  "pk()": [1],
  "a": "a",
  "b": "b",
  "c": "c"
}
*/

-- test: variadic, VALUES, with a few fields
CREATE TABLE test (a TEXT, b TEXT, c TEXT, ...);
INSERT INTO test (b, a, d) VALUES ('b', 'a', 'd');
SELECT pk(), * FROM test;
/* result:
{
  "pk()": [1],
  "a": "a",
  "b": "b",
  "d": "d"
}
*/

-- test: VALUES, no fields, all values
CREATE TABLE test (a TEXT, b TEXT, c TEXT);
INSERT INTO test VALUES ("a", 'b', 'c');
SELECT pk(), * FROM test;
/* result:
{
  "pk()": [1],
  "a": "a",
  "b": "b",
  "c": "c"
}
*/

-- test: VALUES, no fields, few values
CREATE TABLE test (a TEXT, b TEXT, c TEXT);
INSERT INTO test VALUES ("a", 'b');
SELECT pk(), * FROM test;
/* result:
{
  "pk()": [1],
  "a": "a",
  "b": "b"
}
*/

-- test: variadic, VALUES, no fields, few values
CREATE TABLE test (a TEXT, b TEXT, c TEXT, ...);
INSERT INTO test VALUES ("a", 'b');
SELECT pk(), * FROM test;
/* result:
{
  "pk()": [1],
  "a": "a",
  "b": "b"
}
*/

-- test: variadic, VALUES, no fields, all values and more
CREATE TABLE test (a TEXT, b TEXT, c TEXT, ...);
INSERT INTO test VALUES ("a", 'b', 'c', 'd', 'e');
-- error:

-- test: VALUES, ident
CREATE TABLE test (a TEXT, b TEXT, c TEXT);
INSERT INTO test (a) VALUES (a);
-- error: field not found

-- test: VALUES, ident string
CREATE TABLE test (a TEXT, b TEXT, c TEXT);
INSERT INTO test (a) VALUES (`a`);
-- error: field not found

-- test: VALUES, fields ident string
CREATE TABLE test (a TEXT, `foo bar` TEXT);
INSERT INTO test (a, `foo bar`) VALUES ('a', 'foo bar');
SELECT pk(), * FROM test;
/* result:
{
  "pk()": [1],
  "a": "a",
  "foo bar": "foo bar"
}
*/

-- test: VALUES, array
CREATE TABLE test (a TEXT, b TEXT, c ARRAY);
INSERT INTO test (a, b, c) VALUES ("a", 'b', [1, 2, 3]);
SELECT pk(), * FROM test;
/* result:
{
  "pk()": [1],
  "a": "a",
  "b":"b",
  "c": [1.0, 2.0, 3.0]
}
*/

-- test: VALUES, generic object
CREATE TABLE test (a TEXT, b TEXT, c OBJECT);
INSERT INTO test (a, b, c) VALUES ("a", 'b', {c: 1, d: c + 1});
SELECT pk(), * FROM test;
/* result:
{
  "pk()": [1],
  "a": "a",
  "b": "b",
  "c": {
    "c": 1.0,
    "d": 2.0
  }
}
*/

