-- test: INTEGER
CREATE TABLE test (pk INT PRIMARY KEY, a INTEGER);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (pk INTEGER NOT NULL, a INTEGER, CONSTRAINT test_pk PRIMARY KEY (pk))"
}
*/

-- test: BIGINT
CREATE TABLE test (pk INT PRIMARY KEY, a BIGINT);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (pk INTEGER NOT NULL, a BIGINT, CONSTRAINT test_pk PRIMARY KEY (pk))"
}
*/

-- test: DOUBLE
CREATE TABLE test (pk INT PRIMARY KEY, a DOUBLE PRECISION);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (pk INTEGER NOT NULL, a DOUBLE PRECISION, CONSTRAINT test_pk PRIMARY KEY (pk))"
}
*/

-- test: BOOLEAN
CREATE TABLE test (pk INT PRIMARY KEY, a BOOLEAN);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (pk INTEGER NOT NULL, a BOOLEAN, CONSTRAINT test_pk PRIMARY KEY (pk))"
}
*/

-- test: BYTEA
CREATE TABLE test (pk INT PRIMARY KEY, a BYTEA);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (pk INTEGER NOT NULL, a BYTEA, CONSTRAINT test_pk PRIMARY KEY (pk))"
}
*/

-- test: TEXT
CREATE TABLE test (pk INT PRIMARY KEY, a TEXT);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (pk INTEGER NOT NULL, a TEXT, CONSTRAINT test_pk PRIMARY KEY (pk))"
}
*/

-- test: duplicate type
CREATE TABLE test (pk INT PRIMARY KEY, a INT, a TEXT);
-- error:

-- test: INTEGER ALIAS: INT
CREATE TABLE test (pk INT PRIMARY KEY, a INT);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (pk INTEGER NOT NULL, a INTEGER, CONSTRAINT test_pk PRIMARY KEY (pk))"
}
*/

-- test: INT ALIAS: TINYINT
CREATE TABLE test (pk INT PRIMARY KEY, a TINYINT);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (pk INTEGER NOT NULL, a INTEGER, CONSTRAINT test_pk PRIMARY KEY (pk))"
}
*/

-- test: INT ALIAS: mediumint
CREATE TABLE test (pk INT PRIMARY KEY, a INTEGER);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (pk INTEGER NOT NULL, a INTEGER, CONSTRAINT test_pk PRIMARY KEY (pk))"
}
*/

-- test: INT ALIAS: mediumint
CREATE TABLE test (pk INT PRIMARY KEY, a mediumint);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (pk INTEGER NOT NULL, a INTEGER, CONSTRAINT test_pk PRIMARY KEY (pk))"
}
*/

-- test: INT ALIAS: INT2
CREATE TABLE test (pk INT PRIMARY KEY, a int2);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (pk INTEGER NOT NULL, a INTEGER, CONSTRAINT test_pk PRIMARY KEY (pk))"
}
*/

-- test: BIGINT ALIAS: INT8
CREATE TABLE test (pk INT PRIMARY KEY, a int8);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (pk INTEGER NOT NULL, a BIGINT, CONSTRAINT test_pk PRIMARY KEY (pk))"
}
*/

-- test: BOOLEAN ALIAS: BOOL
CREATE TABLE test (pk INT PRIMARY KEY, a BOOL);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (pk INTEGER NOT NULL, a BOOLEAN, CONSTRAINT test_pk PRIMARY KEY (pk))"
}
*/

-- test: TEXT ALIAS: VARCHAR(n)
CREATE TABLE test (pk INT PRIMARY KEY, a VARCHAR(255));
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (pk INTEGER NOT NULL, a TEXT, CONSTRAINT test_pk PRIMARY KEY (pk))"
}
*/

-- test: TEXT ALIAS: character(n)
CREATE TABLE test (pk INT PRIMARY KEY, a character(255));
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (pk INTEGER NOT NULL, a TEXT, CONSTRAINT test_pk PRIMARY KEY (pk))"
}
*/
