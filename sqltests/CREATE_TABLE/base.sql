-- test: basic
CREATE TABLE test(a int primary key);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER NOT NULL, CONSTRAINT test_pk PRIMARY KEY (a))"
}
*/

-- test: duplicate
CREATE TABLE test(a int primary key);
CREATE TABLE test(a int primary key);
-- error:

-- test: if not exists
CREATE TABLE test(a int primary key);
CREATE TABLE IF NOT EXISTS test(b int primary key);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER NOT NULL, CONSTRAINT test_pk PRIMARY KEY (a))"
}
*/

-- test: if not exists, twice
CREATE TABLE IF NOT EXISTS test(a int primary key);
CREATE TABLE IF NOT EXISTS test(a int primary key);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER NOT NULL, CONSTRAINT test_pk PRIMARY KEY (a))"
}
*/
