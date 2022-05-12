-- test: Should generate a key by default
CREATE TABLE test (a TEXT);
INSERT INTO test (a) VALUES ("foo"), ("bar");
SELECT pk(), a FROM test;
/* result:
{
  "pk()": [1],
  "a": "foo"
}
{
  "pk()": [2],
  "a": "bar"
}
*/

-- test: Should use the right field if primary key is specified
CREATE TABLE test (a (b TEXT PRIMARY KEY));
INSERT INTO test (a) VALUES ({b: "foo"}), ({b:"bar"});
SELECT pk(), a FROM test;
/* result:
{
  "pk()": ["bar"],
  "a": {
    "b": "bar"
  }
}
{
  "pk()": ["foo"],
  "a": {
    "b": "foo"
  }
}
*/

-- test: Should fail if Pk not found
CREATE TABLE test (a PRIMARY KEY, b INT);
INSERT INTO test (b) VALUES (1);
-- error:

-- test: Should fail if Pk NULL
CREATE TABLE test (a PRIMARY KEY, b INT);
INSERT INTO test (a, b) VALUES (NULL, 1);
-- error:
