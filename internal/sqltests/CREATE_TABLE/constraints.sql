-- test: no constraint
CREATE TABLE test(a, b, c);
SELECT name, sql FROM __genji_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a ANY, b ANY, c ANY)"
}
*/

-- test: type
CREATE TABLE test(a INTEGER);
SELECT name, sql FROM __genji_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER)"
}
*/

-- test: NOT NULL
CREATE TABLE test(a NOT NULL);
SELECT name, sql FROM __genji_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a ANY NOT NULL)"
}
*/

-- test: default
CREATE TABLE test(a DEFAULT 10);
SELECT name, sql FROM __genji_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a ANY DEFAULT 10)"
}
*/

-- test: unique
CREATE TABLE test(a UNIQUE);
SELECT name, sql FROM __genji_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a ANY, CONSTRAINT test_a_unique UNIQUE (a))"
}
*/

-- test: check
CREATE TABLE test(a CHECK(a > 10));
SELECT name, sql FROM __genji_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a ANY, CONSTRAINT test_check CHECK (a > 10))"
}
*/

-- test: all compatible constraints
CREATE TABLE test(a INT PRIMARY KEY NOT NULL DEFAULT 10 UNIQUE CHECK(a >= 10));
SELECT name, sql FROM __genji_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER NOT NULL DEFAULT 10, CONSTRAINT test_pk PRIMARY KEY (a), CONSTRAINT test_a_unique UNIQUE (a), CONSTRAINT test_check CHECK (a >= 10))"
}
*/
