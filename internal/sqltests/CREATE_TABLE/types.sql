-- test: INTEGER
CREATE TABLE test (a INTEGER);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER)"
}
*/

-- test: DOUBLE
CREATE TABLE test (a DOUBLE);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a DOUBLE)"
}
*/

-- test: BOOLEAN
CREATE TABLE test (a BOOLEAN);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a BOOLEAN)"
}
*/

-- test: BLOB
CREATE TABLE test (a BLOB);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a BLOB)"
}
*/

-- test: TEXT
CREATE TABLE test (a TEXT);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a TEXT)"
}
*/

-- test: ARRAY
CREATE TABLE test (a ARRAY);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a ARRAY)"
}
*/

-- test: OBJECT
CREATE TABLE test (a OBJECT);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a (...))"
}
*/

-- test: ANY
CREATE TABLE test (a ANY);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a ANY)"
}
*/

-- test: duplicate type
CREATE TABLE test (a INT, a TEXT);
-- error:

-- test: INTEGER ALIAS: INT
CREATE TABLE test (a INT);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER)"
}
*/

-- test: INT ALIAS: TINYINT
CREATE TABLE test (a INTEGER);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER)"
}
*/

-- test: INT ALIAS: BIGINT
CREATE TABLE test (a BIGINT);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER)"
}
*/

-- test: INT ALIAS: mediumint
CREATE TABLE test (a mediumint);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER)"
}
*/

-- test: INT ALIAS: INT2
CREATE TABLE test (a int2);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER)"
}
*/

-- test: INT ALIAS: INT8
CREATE TABLE test (a int8);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER)"
}
*/

-- test: BOOLEAN ALIAS: BOOL
CREATE TABLE test (a BOOL);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a BOOLEAN)"
}
*/

-- test: TEXT ALIAS: VARCHAR(n)
CREATE TABLE test (a VARCHAR(255));
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a TEXT)"
}
*/

-- test: TEXT ALIAS: character(n)
CREATE TABLE test (a character(255));
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a TEXT)"
}
*/
