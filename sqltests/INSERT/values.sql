-- test: VALUES, with all columns
CREATE TABLE test (a TEXT, b TEXT, c TEXT);
INSERT INTO test (a, b, c) VALUES ('a', 'b', 'c');
SELECT * FROM test;
/* result:
{
  "a": "a",
  "b": "b",
  "c": "c"
}
*/

-- test: VALUES, with a few columns
CREATE TABLE test (a TEXT, b TEXT, c TEXT);
INSERT INTO test (b, a) VALUES ('b', 'a');
SELECT * FROM test;
/* result:
{
  "a": "a",
  "b": "b",
  "c": null
}
*/

-- test: VALUES, with too many columns
CREATE TABLE test (a TEXT, b TEXT, c TEXT);
INSERT INTO test (b, a, c, d) VALUES ('b', 'a', 'c', 'd');
-- error: table has no column d

-- test: VALUES, no columns, all values
CREATE TABLE test (a TEXT, b TEXT, c TEXT);
INSERT INTO test VALUES ("a", 'b', 'c');
SELECT * FROM test;
/* result:
{
  "a": "a",
  "b": "b",
  "c": "c"
}
*/

-- test: VALUES, no columns, few values
CREATE TABLE test (a TEXT, b TEXT, c TEXT);
INSERT INTO test VALUES ('a', 'b');
SELECT * FROM test;
/* result:
{
  "a": "a",
  "b": "b",
  "c": null
}
*/

-- test: VALUES, ident
CREATE TABLE test (a TEXT, b TEXT, c TEXT);
INSERT INTO test (a) VALUES (a);
-- error: no table specified

-- test: VALUES, ident string
CREATE TABLE test (a TEXT, b TEXT, c TEXT);
INSERT INTO test (a) VALUES (`a`);
-- error: no table specified

-- test: VALUES, columns ident string
CREATE TABLE test (a TEXT, `foo bar` TEXT);
INSERT INTO test (a, `foo bar`) VALUES ('a', 'foo bar');
SELECT * FROM test;
/* result:
{
  "a": "a",
  "foo bar": "foo bar"
}
*/


