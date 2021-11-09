-- test: as field constraint
CREATE TABLE test (
    a CHECK(a > 10) CHECK(b < 10)
);
SELECT name, type, sql FROM __genji_catalog WHERE name = "test";
/* result:
{
  name: "test",
  type: "table",
  sql: "CREATE TABLE test (CHECK (a > 10), CHECK (b < 10))"
}
*/

-- test: as field constraint, with other constraints
CREATE TABLE test (
    a INT CHECK (a > 10) DEFAULT 100 NOT NULL PRIMARY KEY
);
SELECT name, type, sql FROM __genji_catalog WHERE name = "test";
/* result:
{
  name: "test",
  type: "table",
  sql: "CREATE TABLE test (a INTEGER NOT NULL DEFAULT 100, CHECK (a > 10), PRIMARY KEY (a))"
}
*/

-- test: as field constraint, no parentheses
CREATE TABLE test (
    a INT CHECK a > 10
);
-- error:

-- test: as field constraint, incompatible default value
CREATE TABLE test (
    a INT CHECK (a > 10) DEFAULT 0
);
SELECT name, type, sql FROM __genji_catalog WHERE name = "test";
/* result:
{
  name: "test",
  type: "table",
  sql: "CREATE TABLE test (a INTEGER DEFAULT 0, CHECK (a > 10))"
}
*/

-- test: as field constraint, reference other fields
CREATE TABLE test (
    a INT CHECK (a > 10 AND b < 10),
    b INT
);
SELECT name, type, sql FROM __genji_catalog WHERE name = "test";
/* result:
{
  name: "test",
  type: "table",
  sql: "CREATE TABLE test (a INTEGER, b INTEGER, CHECK (a > 10 AND b < 10))"
}
*/

-- test: as table constraint
CREATE TABLE test (
    a INT,
    CHECK (a > 10),
    CHECK (a > 20)
);
SELECT name, type, sql FROM __genji_catalog WHERE name = "test";
/* result:
{
  name: "test",
  type: "table",
  sql: "CREATE TABLE test (a INTEGER, CHECK (a > 10), CHECK (a > 20))"
}
*/
