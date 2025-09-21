-- test: basic
CREATE TABLE test(a INTEGER PRIMARY KEY);
SELECT name, sql FROM __chai_catalog WHERE type = 'table' AND name = 'test';
/* result:
{
  "name": 'test',
  "sql": 'CREATE TABLE test (a INTEGER NOT NULL, CONSTRAINT test_pk PRIMARY KEY (a))'
}
*/

-- test: with ASC order
CREATE TABLE test(a INT PRIMARY KEY ASC);
SELECT name, sql FROM __chai_catalog WHERE type = 'table' AND name = 'test';
/* result:
{
  "name": 'test',
  "sql": 'CREATE TABLE test (a INTEGER NOT NULL, CONSTRAINT test_pk PRIMARY KEY (a))'
}
*/

-- test: with DESC order
CREATE TABLE test(a INT PRIMARY KEY DESC);
SELECT name, sql FROM __chai_catalog WHERE type = 'table' AND name = 'test';
/* result:
{
  "name": 'test',
  "sql": 'CREATE TABLE test (a INTEGER NOT NULL, CONSTRAINT test_pk PRIMARY KEY (a DESC))'
}
*/

-- test: twice
CREATE TABLE test(a INT PRIMARY KEY PRIMARY KEY);
-- error:

-- test: duplicate
CREATE TABLE test(a INT PRIMARY KEY, b INT PRIMARY KEY);
-- error:

-- test: table constraint: one column
CREATE TABLE test(a INT, PRIMARY KEY(a));
SELECT name, sql FROM __chai_catalog WHERE type = 'table' AND name = 'test';
/* result:
{
  "name": 'test',
  "sql": 'CREATE TABLE test (a INTEGER NOT NULL, CONSTRAINT test_pk PRIMARY KEY (a))'
}
*/

-- test: table constraint: multiple columns
CREATE TABLE test(a INT, b INT, PRIMARY KEY(a, b));
SELECT name, sql FROM __chai_catalog WHERE type = 'table' AND name = 'test';
/* result:
{
  "name": 'test',
  "sql": 'CREATE TABLE test (a INTEGER NOT NULL, b INTEGER NOT NULL, CONSTRAINT test_pk PRIMARY KEY (a, b))'
}
*/

-- test: table constraint: multiple columns: with order
CREATE TABLE test(a INT, b INT, c INT, PRIMARY KEY(a DESC, b, c ASC));
SELECT name, sql FROM __chai_catalog WHERE type = 'table' AND name = 'test';
/* result:
{
  "name": 'test',
  "sql": 'CREATE TABLE test (a INTEGER NOT NULL, b INTEGER NOT NULL, c INTEGER NOT NULL, CONSTRAINT test_pk PRIMARY KEY (a DESC, b, c))'
}
*/

-- test: table constraint: undeclared columns
CREATE TABLE test(a INT, b INT, PRIMARY KEY(a, b, c));
-- error:

-- test: table constraint: same column twice
CREATE TABLE test(a INT, b INT, PRIMARY KEY(a, a));
-- error:

-- test: table constraint: same column twice, column constraint + table constraint
CREATE TABLE test(a INT PRIMARY KEY, b INT, PRIMARY KEY(a));
-- error:

-- test: table constraint: duplicate
CREATE TABLE test(a INT PRIMARY KEY, b INT, PRIMARY KEY(b));
-- error:

-- test: named table constraint preserved for PRIMARY KEY
CREATE TABLE test(a INTEGER, b INTEGER, CONSTRAINT my_pk PRIMARY KEY (a DESC, b));
SELECT name, sql FROM __chai_catalog WHERE type = 'table' AND name = 'test';
/* result:
{
  "name": 'test',
  "sql": 'CREATE TABLE test (a INTEGER NOT NULL, b INTEGER NOT NULL, CONSTRAINT my_pk PRIMARY KEY (a DESC, b))'
}
*/
