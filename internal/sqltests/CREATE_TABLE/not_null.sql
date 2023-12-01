-- test: basic
CREATE TABLE test(a NOT NULL);
SELECT name, sql FROM __genji_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a ANY NOT NULL)"
}
*/

-- test: with type
CREATE TABLE test(a INT NOT NULL);
SELECT name, sql FROM __genji_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER NOT NULL)"
}
*/

-- test: not null twice
CREATE TABLE test(a INT NOT NULL NOT NULL);
-- error:
