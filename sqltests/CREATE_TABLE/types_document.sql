-- test: no keyword
CREATE TABLE test (a (b int));
SELECT name, sql FROM __genji_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a (b INTEGER))"
}
*/

-- test: with keyword
CREATE TABLE test (a DOCUMENT (b int));
SELECT name, sql FROM __genji_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a (b INTEGER))"
}
*/

-- test: with ellipsis
CREATE TABLE test (a DOCUMENT (b int, ...));
SELECT name, sql FROM __genji_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a (b INTEGER, ...))"
}
*/
