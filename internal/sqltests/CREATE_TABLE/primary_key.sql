-- test: basic
CREATE TABLE test(a PRIMARY KEY);
SELECT name, sql FROM __genji_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a ANY NOT NULL, CONSTRAINT test_pk PRIMARY KEY (a))"
}
*/

-- test: with type
CREATE TABLE test(a INT PRIMARY KEY);
SELECT name, sql FROM __genji_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER NOT NULL, CONSTRAINT test_pk PRIMARY KEY (a))"
}
*/

-- test: with ASC order
CREATE TABLE test(a INT PRIMARY KEY ASC);
SELECT name, sql FROM __genji_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER NOT NULL, CONSTRAINT test_pk PRIMARY KEY (a))"
}
*/

-- test: with DESC order
CREATE TABLE test(a INT PRIMARY KEY DESC);
SELECT name, sql FROM __genji_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER NOT NULL, CONSTRAINT test_pk PRIMARY KEY (a DESC))"
}
*/

-- test: twice
CREATE TABLE test(a INT PRIMARY KEY PRIMARY KEY);
-- error:

-- test: duplicate
CREATE TABLE test(a INT PRIMARY KEY, b INT PRIMARY KEY);
-- error:

-- test: table constraint: one field
CREATE TABLE test(a INT, PRIMARY KEY(a));
SELECT name, sql FROM __genji_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER NOT NULL, CONSTRAINT test_pk PRIMARY KEY (a))"
}
*/

-- test: table constraint: multiple fields
CREATE TABLE test(a INT, b INT, PRIMARY KEY(a, b));
SELECT name, sql FROM __genji_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER NOT NULL, b INTEGER NOT NULL, CONSTRAINT test_pk PRIMARY KEY (a, b))"
}
*/

-- test: table constraint: nested fields
CREATE TABLE test(a (b INT), PRIMARY KEY(a.b));
SELECT name, sql FROM __genji_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a (b INTEGER NOT NULL), CONSTRAINT test_pk PRIMARY KEY (a.b))"
}
*/


-- test: table constraint: multiple fields: with order
CREATE TABLE test(a INT, b INT, c (d INT), PRIMARY KEY(a DESC, b, c.d ASC));
SELECT name, sql FROM __genji_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER NOT NULL, b INTEGER NOT NULL, c (d INTEGER NOT NULL), CONSTRAINT test_pk PRIMARY KEY (a DESC, b, c.d))"
}
*/

-- test: table constraint: undeclared fields
CREATE TABLE test(a INT, b INT, PRIMARY KEY(a, b, c));
-- error:

-- test: table constraint: same field twice
CREATE TABLE test(a INT, b INT, PRIMARY KEY(a, a));
-- error:

-- test: table constraint: same field twice, field constraint + table constraint
CREATE TABLE test(a INT PRIMARY KEY, b INT, PRIMARY KEY(a));
-- error:

-- test: table constraint: duplicate
CREATE TABLE test(a INT PRIMARY KEY, b INT, PRIMARY KEY(b));
-- error:
