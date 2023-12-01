-- test: document
CREATE TABLE test (a TEXT, b DOUBLE, c BOOLEAN);
INSERT INTO test VALUES {a: 'a', b: 2.3, c: 1 = 1};
SELECT pk(), * FROM test;
/* result:
{
  "pk()": [1],
  "a": "a",
  "b": 2.3,
  "c": true
}
*/

-- test: document, array
CREATE TABLE test (a ARRAY);
INSERT INTO test VALUES {a: [1, 2, 3]};
SELECT pk(), * FROM test;
/* result:
{
  "pk()": [1],
  "a": [
    1.0,
    2.0,
    3.0
  ]
}
*/

-- test: document, strings
CREATE TABLE test (a TEXT, b DOUBLE);
INSERT INTO test VALUES {'a': 'a', b: 2.3};
SELECT pk(), * FROM test;
/* result:
{
  "pk()": [1],
  "a": "a",
  "b": 2.3
}
*/

-- test: document, double quotes
CREATE TABLE test (a TEXT);
INSERT INTO test VALUES {"a": "b"};
SELECT pk(), * FROM test;
/* result:
{
  "pk()": [1],
  "a": "b"
}
*/

-- test: document, references to other field
CREATE TABLE test (a INT, b INT);
INSERT INTO test VALUES {a: 400, b: a * 4};
SELECT pk(), * FROM test;
/* result:
{
    "pk()":[1],
    "a":400,
    "b":1600
}
*/
