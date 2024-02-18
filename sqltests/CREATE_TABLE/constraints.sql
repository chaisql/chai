-- test: type
CREATE TABLE test(a INTEGER);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER)"
}
*/

-- test: NOT NULL
CREATE TABLE test(a INT NOT NULL);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER NOT NULL)"
}
*/

-- test: default
CREATE TABLE test(a INT DEFAULT 10);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER DEFAULT 10)"
}
*/

-- test: unique
CREATE TABLE test(a INT UNIQUE);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER, CONSTRAINT test_a_unique UNIQUE (a))"
}
*/

-- test: check
CREATE TABLE test(a INT CHECK(a > 10));
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER, CONSTRAINT test_check CHECK (a > 10))"
}
*/

-- test: all compatible constraints
CREATE TABLE test(a INT PRIMARY KEY NOT NULL DEFAULT 10 UNIQUE CHECK(a >= 10));
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER NOT NULL DEFAULT 10, CONSTRAINT test_pk PRIMARY KEY (a), CONSTRAINT test_a_unique UNIQUE (a), CONSTRAINT test_check CHECK (a >= 10))"
}
*/
