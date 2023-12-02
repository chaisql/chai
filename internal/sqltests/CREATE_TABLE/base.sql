-- test: basic
CREATE TABLE test;
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (...)"
}
*/

-- test: duplicate
CREATE TABLE test;
CREATE TABLE test;
-- error:

-- test: if not exists
CREATE TABLE test;
CREATE TABLE IF NOT EXISTS test;
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (...)"
}
*/

-- test: if not exists, twice
CREATE TABLE IF NOT EXISTS test;
CREATE TABLE IF NOT EXISTS test;
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (...)"
}
*/
