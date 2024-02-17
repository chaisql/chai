-- setup:
CREATE TABLE test (a int);

-- test: named index
CREATE INDEX test_a_idx ON test(a);
SELECT name, owner_table_name AS table_name, sql FROM __chai_catalog WHERE type = "index";
/* result:
{
  "name": "test_a_idx",
  "table_name": "test",
  "sql": "CREATE INDEX test_a_idx ON test (a)"
}
*/

-- test: named unique index
CREATE UNIQUE INDEX test_a_idx ON test(a);
SELECT name, owner_table_name AS table_name, sql FROM __chai_catalog WHERE type = "index";
/* result:
{
  "name": "test_a_idx",
  "table_name": "test",
  "sql": "CREATE UNIQUE INDEX test_a_idx ON test (a)"
}
*/

-- test: conflict
CREATE INDEX test_a_idx ON test(a);
CREATE INDEX test_a_idx ON test(a);
-- error:

-- test: conflict with UNIQUE
CREATE INDEX test_a_idx ON test(a);
CREATE UNIQUE INDEX test_a_idx ON test(a);
-- error:

-- test: IF NOT EXISTS
CREATE INDEX test_a_idx ON test(a);
CREATE INDEX IF NOT EXISTS test_a_idx ON test(a);
SELECT name, owner_table_name AS table_name, sql FROM __chai_catalog WHERE type = "index";
/* result:
{
  "name": "test_a_idx",
  "table_name": "test",
  "sql": "CREATE INDEX test_a_idx ON test (a)"
}
*/

-- test: generated name
CREATE INDEX ON test(a);
CREATE INDEX ON test(a);
CREATE INDEX test_a_idx2 ON test(a);
CREATE INDEX ON test(a);
SELECT name, owner_table_name AS table_name, sql FROM __chai_catalog WHERE type = "index" ORDER BY name;
/* result:
{
  "name": "test_a_idx",
  "table_name": "test",
  "sql": "CREATE INDEX test_a_idx ON test (a)"
}
{
  "name": "test_a_idx1",
  "table_name": "test",
  "sql": "CREATE INDEX test_a_idx1 ON test (a)"
}
{
  "name": "test_a_idx2",
  "table_name": "test",
  "sql": "CREATE INDEX test_a_idx2 ON test (a)"
}
{
  "name": "test_a_idx3",
  "table_name": "test",
  "sql": "CREATE INDEX test_a_idx3 ON test (a)"
}
*/

-- test: generated name with IF NOT EXISTS
CREATE INDEX IF NOT EXISTS ON test(a);
-- error:
