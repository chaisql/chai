-- test: basic
CREATE TABLE test(a int);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER)"
}
*/

-- test: duplicate
CREATE TABLE test(a int);
CREATE TABLE test(a int);
-- error:

-- test: if not exists
CREATE TABLE test(a int);
CREATE TABLE IF NOT EXISTS test(b int);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER)"
}
*/

-- test: if not exists, twice
CREATE TABLE IF NOT EXISTS test(a int);
CREATE TABLE IF NOT EXISTS test(a int);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER)"
}
*/
