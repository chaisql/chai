-- test: as field constraint
CREATE TABLE test (
    a CHECK(a > 10)
);
SELECT * FROM __genji_catalog WHERE name = "test";
/* result:
{
  name: "test",
  type: "table",
  sql: "CREATE TABLE test (CHECK(a > 10))"
}
*/

-- test: as field constraint, with other constraints
CREATE TABLE test (
    a INT CHECK (a > 10) DEFAULT 100 NOT NULL PRIMARY KEY
);
SELECT * FROM __genji_catalog WHERE name = "test";
/* result:
{
  name: "test",
  type: "table",
  sql: "CREATE TABLE test (a INTEGER NOT NULL PRIMARY KEY DEFAULT 100, CHECK(a > 10))"
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
SELECT * FROM __genji_catalog WHERE name = "test";
/* result:
{
  name: "test",
  type: "table",
  sql: "CREATE TABLE test (a INTEGER DEFAULT 0, CHECK (a > 10))"
}
*/

-- test: as field constraint, reference other fields
CREATE TABLE test (
    a INT CHECK (a > 10 AND b < 10)
    b INT
);
SELECT * FROM __genji_catalog WHERE name = "test";
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
    CHECK (a > 20),
);
/* result:
{
  name: "test",
  type: "table",
  sql: "CREATE TABLE test (a INTEGER, CHECK (a > 10), CHECK (a > 20))"
}
*/
