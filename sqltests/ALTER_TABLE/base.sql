-- setup:
CREATE TABLE test(a int primary key);

-- test: rename
ALTER TABLE test RENAME TO test2;
SELECT name, sql FROM __genji_catalog WHERE type = "table" AND (name = "test2" OR name = "test");
/* result:
{
  "name": "test2",
  "sql": "CREATE TABLE test2 (a INTEGER, PRIMARY KEY (a))"
}
*/

-- test: non-existing
ALTER TABLE unknown RENAME TO test2;
-- error:

-- test: duplicate
CREATE TABLE test2;
ALTER TABLE test2 RENAME TO test;
-- error:
