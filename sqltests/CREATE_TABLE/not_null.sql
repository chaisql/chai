-- test: basic
CREATE TABLE test(a INTEGER NOT NULL);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER NOT NULL)"
}
*/

-- test: not null twice
CREATE TABLE test(a INT NOT NULL NOT NULL);
-- error:
