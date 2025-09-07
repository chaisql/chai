-- test: as column constraint
CREATE TABLE test (
    a INT PRIMARY KEY CHECK(a > 10 AND a < 20)
);
SELECT name, type, sql FROM __chai_catalog WHERE name = "test";
/* result:
{
  name: "test",
  type: "table",
  sql: "CREATE TABLE test (a INTEGER NOT NULL, CONSTRAINT test_pk PRIMARY KEY (a), CONSTRAINT test_check CHECK (a > 10 AND a < 20))"
}
*/

-- test: as column constraint: undeclared column
CREATE TABLE test (
    a INT CHECK(b > 10)
);
-- error:

-- test: as column constraint, with other constraints
CREATE TABLE test (
    a INT CHECK (a > 10) DEFAULT 100 NOT NULL PRIMARY KEY
);
SELECT name, type, sql FROM __chai_catalog WHERE name = "test";
/* result:
{
  name: "test",
  type: "table",
  sql: "CREATE TABLE test (a INTEGER NOT NULL DEFAULT 100, CONSTRAINT test_check CHECK (a > 10), CONSTRAINT test_pk PRIMARY KEY (a))"
}
*/

-- test: as column constraint, no parentheses
CREATE TABLE test (
    a INT PRIMARY KEY CHECK a > 10
);
-- error:

-- test: as column constraint, incompatible default value
CREATE TABLE test (
    a INT PRIMARY KEY CHECK (a > 10) DEFAULT 0
);
SELECT name, type, sql FROM __chai_catalog WHERE name = "test";
/* result:
{
  name: "test",
  type: "table",
  sql: "CREATE TABLE test (a INTEGER NOT NULL DEFAULT 0, CONSTRAINT test_pk PRIMARY KEY (a), CONSTRAINT test_check CHECK (a > 10))"
}
*/

-- test: as column constraint, reference other columns
CREATE TABLE test (
    a INT PRIMARY KEY CHECK (a > 10 AND b < 10),
    b INT
);
SELECT name, type, sql FROM __chai_catalog WHERE name = "test";
/* result:
{
  name: "test",
  type: "table",
  sql: "CREATE TABLE test (a INTEGER NOT NULL, b INTEGER, CONSTRAINT test_pk PRIMARY KEY (a), CONSTRAINT test_check CHECK (a > 10 AND b < 10))"
}
*/

-- test: as table constraint
CREATE TABLE test (
    a INT PRIMARY KEY,
    CHECK (a > 10),
    CHECK (a > 20)
);
SELECT name, type, sql FROM __chai_catalog WHERE name = "test";
/* result:
{
  name: "test",
  type: "table",
  sql: "CREATE TABLE test (a INTEGER NOT NULL, CONSTRAINT test_pk PRIMARY KEY (a), CONSTRAINT test_check CHECK (a > 10), CONSTRAINT test_check1 CHECK (a > 20))"
}
*/
