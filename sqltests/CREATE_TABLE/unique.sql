-- test: ANY
CREATE TABLE test(a UNIQUE);
SELECT name, sql 
FROM __genji_catalog 
WHERE 
    (type = "table" AND name = "test") 
  OR
    (type = "index" AND name = "test_a_idx");
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a ANY, CONSTRAINT test_a_unique UNIQUE (a))"
}
{
  "name": "test_a_idx",
  "sql": "CREATE UNIQUE INDEX test_a_idx ON test (a)"
}
*/

-- test: with type
CREATE TABLE test(a INT UNIQUE);
SELECT name, sql 
FROM __genji_catalog 
WHERE 
    (type = "table" AND name = "test") 
  OR
    (type = "index" AND name = "test_a_idx");
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER, CONSTRAINT test_a_unique UNIQUE (a))"
}
{
  "name": "test_a_idx",
  "sql": "CREATE UNIQUE INDEX test_a_idx ON test (a)"
}
*/

-- test: multiple
CREATE TABLE test(a INT UNIQUE, b DOUBLE UNIQUE);
SELECT name, sql 
FROM __genji_catalog 
WHERE 
    (type = "table" AND name = "test") 
  OR
    (type = "index" AND owner.table_name = "test");
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER, b DOUBLE, CONSTRAINT test_a_unique UNIQUE (a), CONSTRAINT test_b_unique UNIQUE (b))"
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

-- test: table constraint: one field
CREATE TABLE test(a INT, UNIQUE(a));
SELECT name, sql
FROM __genji_catalog 
WHERE 
    (type = "table" AND name = "test") 
  OR
    (type = "index" AND name = "test_a_idx");
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER, CONSTRAINT test_a_unique UNIQUE (a))"
}
{
  "name": "test_a_idx",
  "sql": "CREATE UNIQUE INDEX test_a_idx ON test (a)"
}
*/

-- test: table constraint: multiple fields
CREATE TABLE test(a INT, b INT, UNIQUE(a, b));
SELECT name, sql 
FROM __genji_catalog 
WHERE 
    (type = "table" AND name = "test") 
  OR
    (type = "index" AND name = "test_a_b_idx");
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER, b INTEGER, CONSTRAINT test_a_b_unique UNIQUE (a, b))"
}
{
  "name": "test_a_b_idx",
  "sql": "CREATE UNIQUE INDEX test_a_b_idx ON test (a, b)"
}
*/

-- test: table constraint: multiple fields with order
CREATE TABLE test(a INT, b INT, c INT, UNIQUE(a DESC, b ASC, c));
SELECT name, sql 
FROM __genji_catalog 
WHERE 
    (type = "table" AND name = "test") 
  OR
    (type = "index" AND name = "test_a_b_c_idx");
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER, b INTEGER, c INTEGER, CONSTRAINT test_a_b_c_unique UNIQUE (a DESC, b, c))"
}
{
  "name": "test_a_b_c_idx",
  "sql": "CREATE UNIQUE INDEX test_a_b_c_idx ON test (a DESC, b, c)"
}
*/

-- test: table constraint: undeclared field
CREATE TABLE test(a INT, UNIQUE(b));
-- error:

-- test: table constraint: undeclared fields
CREATE TABLE test(a INT, b INT, UNIQUE(a, b, c));
-- error:

-- test: table constraint: same field twice
CREATE TABLE test(a INT, b INT, UNIQUE(a, a));
-- error:

-- test: table constraint: same field twice, field constraint + table constraint
CREATE TABLE test(a INT UNIQUE, b INT, UNIQUE(a));
-- error:

-- test: table constraint: different fields
CREATE TABLE test(a INT UNIQUE, b INT, UNIQUE(b));
SELECT name, sql 
FROM __genji_catalog 
WHERE 
    (type = "table" AND name = "test") 
  OR
    (type = "index" AND owner.table_name = "test");
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER, b INTEGER, CONSTRAINT test_a_unique UNIQUE (a), CONSTRAINT test_b_unique UNIQUE (b))"
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
