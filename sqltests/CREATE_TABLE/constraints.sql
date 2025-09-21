-- test: type
CREATE TABLE test(pk INT PRIMARY KEY, a INTEGER);
SELECT name, sql FROM __chai_catalog WHERE type = 'table' AND name = 'test';
/* result:
{
  "name": 'test',
  "sql": 'CREATE TABLE test (pk INTEGER NOT NULL, a INTEGER, CONSTRAINT test_pk PRIMARY KEY (pk))'
}
*/

-- test: NOT NULL
CREATE TABLE test(pk INT PRIMARY KEY, a INT NOT NULL);
SELECT name, sql FROM __chai_catalog WHERE type = 'table' AND name = 'test';
/* result:
{
  "name": 'test',
  "sql": 'CREATE TABLE test (pk INTEGER NOT NULL, a INTEGER NOT NULL, CONSTRAINT test_pk PRIMARY KEY (pk))'
}
*/

-- test: default
CREATE TABLE test(pk INT PRIMARY KEY, a INT DEFAULT 10);
SELECT name, sql FROM __chai_catalog WHERE type = 'table' AND name = 'test';
/* result:
{
  "name": 'test',
  "sql": 'CREATE TABLE test (pk INTEGER NOT NULL, a INTEGER DEFAULT 10, CONSTRAINT test_pk PRIMARY KEY (pk))'
}
*/

-- test: unique
CREATE TABLE test(pk INT PRIMARY KEY, a INT UNIQUE);
SELECT name, sql FROM __chai_catalog WHERE type = 'table' AND name = 'test';
/* result:
{
  "name": 'test',
  "sql": 'CREATE TABLE test (pk INTEGER NOT NULL, a INTEGER, CONSTRAINT test_pk PRIMARY KEY (pk), CONSTRAINT test_a_unique UNIQUE (a))'
}
*/

-- test: check
CREATE TABLE test(pk INT PRIMARY KEY, a INT CHECK(a > 10));
SELECT name, sql FROM __chai_catalog WHERE type = 'table' AND name = 'test';
/* result:
{
  "name": 'test',
  "sql": 'CREATE TABLE test (pk INTEGER NOT NULL, a INTEGER, CONSTRAINT test_pk PRIMARY KEY (pk), CONSTRAINT test_check CHECK (a > 10))'
}
*/

-- test: all compatible constraints
CREATE TABLE test(a INT PRIMARY KEY NOT NULL DEFAULT 10 UNIQUE CHECK(a >= 10));
SELECT name, sql FROM __chai_catalog WHERE type = 'table' AND name = 'test';
/* result:
{
  "name": 'test',
  "sql": 'CREATE TABLE test (a INTEGER NOT NULL DEFAULT 10, CONSTRAINT test_pk PRIMARY KEY (a), CONSTRAINT test_a_unique UNIQUE (a), CONSTRAINT test_check CHECK (a >= 10))'
}
*/
