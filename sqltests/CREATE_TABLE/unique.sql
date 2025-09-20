-- test: with type
CREATE TABLE test(pk INT PRIMARY KEY, a INT UNIQUE);
SELECT name, sql 
FROM __chai_catalog 
WHERE 
    (type = "table" AND name = "test") 
  OR
    (type = "index" AND name = "test_a_idx");
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (pk INTEGER NOT NULL, a INTEGER, CONSTRAINT test_pk PRIMARY KEY (pk), CONSTRAINT test_a_unique UNIQUE (a))"
}
{
  "name": "test_a_idx",
  "sql": "CREATE UNIQUE INDEX test_a_idx ON test (a)"
}
*/

-- test: multiple
CREATE TABLE test(pk INT PRIMARY KEY, a INT UNIQUE, b DOUBLE PRECISION UNIQUE);
SELECT name, sql 
FROM __chai_catalog 
WHERE 
    (type = "table" AND name = "test") 
  OR
    (type = "index" AND owner_table_name = "test");
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (pk INTEGER NOT NULL, a INTEGER, b DOUBLE PRECISION, CONSTRAINT test_pk PRIMARY KEY (pk), CONSTRAINT test_a_unique UNIQUE (a), CONSTRAINT test_b_unique UNIQUE (b))"
}
{
  "name": "test_a_idx",
  "sql": "CREATE UNIQUE INDEX test_a_idx ON test (a)"
}
{
  "name": "test_b_idx",
  "sql": "CREATE UNIQUE INDEX test_b_idx ON test (b)"
}
*/

-- test: table constraint: one column
CREATE TABLE test(a INT PRIMARY KEY, UNIQUE(a));
SELECT name, sql
FROM __chai_catalog 
WHERE 
    (type = "table" AND name = "test") 
  OR
    (type = "index" AND name = "test_a_idx");
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER NOT NULL, CONSTRAINT test_pk PRIMARY KEY (a), CONSTRAINT test_a_unique UNIQUE (a))"
}
{
  "name": "test_a_idx",
  "sql": "CREATE UNIQUE INDEX test_a_idx ON test (a)"
}
*/

-- test: table constraint: multiple columns
CREATE TABLE test(a INT PRIMARY KEY, b INT, UNIQUE(a, b));
SELECT name, sql 
FROM __chai_catalog 
WHERE 
    (type = "table" AND name = "test") 
  OR
    (type = "index" AND name = "test_a_b_idx");
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER NOT NULL, b INTEGER, CONSTRAINT test_pk PRIMARY KEY (a), CONSTRAINT test_a_b_unique UNIQUE (a, b))"
}
{
  "name": "test_a_b_idx",
  "sql": "CREATE UNIQUE INDEX test_a_b_idx ON test (a, b)"
}
*/

-- test: table constraint: multiple columns with order
CREATE TABLE test(a INT PRIMARY KEY, b INT, c INT, UNIQUE(a DESC, b ASC, c));
SELECT name, sql 
FROM __chai_catalog 
WHERE 
    (type = "table" AND name = "test") 
  OR
    (type = "index" AND name = "test_a_b_c_idx");
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER NOT NULL, b INTEGER, c INTEGER, CONSTRAINT test_pk PRIMARY KEY (a), CONSTRAINT test_a_b_c_unique UNIQUE (a DESC, b, c))"
}
{
  "name": "test_a_b_c_idx",
  "sql": "CREATE UNIQUE INDEX test_a_b_c_idx ON test (a DESC, b, c)"
}
*/

-- test: table constraint: undeclared column
CREATE TABLE test(a INT, UNIQUE(b));
-- error:

-- test: table constraint: undeclared columns
CREATE TABLE test(a INT, b INT, UNIQUE(a, b, c));
-- error:

-- test: table constraint: same column twice
CREATE TABLE test(a INT, b INT, UNIQUE(a, a));
-- error:

-- test: table constraint: same column twice, column constraint + table constraint
CREATE TABLE test(a INT UNIQUE, b INT, UNIQUE(a));
-- error:

-- test: table constraint: different columns
CREATE TABLE test(a INT UNIQUE, b INT PRIMARY KEY, UNIQUE(b));
SELECT name, sql 
FROM __chai_catalog 
WHERE 
    (type = "table" AND name = "test") 
  OR
    (type = "index" AND owner_table_name = "test");
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER, b INTEGER NOT NULL, CONSTRAINT test_a_unique UNIQUE (a), CONSTRAINT test_pk PRIMARY KEY (b), CONSTRAINT test_b_unique UNIQUE (b))"
}
{
  "name": "test_a_idx",
  "sql": "CREATE UNIQUE INDEX test_a_idx ON test (a)"
}
{
  "name": "test_b_idx",
  "sql": "CREATE UNIQUE INDEX test_b_idx ON test (b)"
}
*/
