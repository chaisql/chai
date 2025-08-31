-- test: same type
CREATE TABLE test(a INT DEFAULT 10);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER DEFAULT 10)"
}
*/

-- test: compatible type
CREATE TABLE test(a DOUBLE DEFAULT 10);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a DOUBLE DEFAULT 10)"
}
*/

-- test: expr
CREATE TABLE test(a DOUBLE DEFAULT 1 + 4 / 4);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a DOUBLE DEFAULT 1 + 4 / 4)"
}
*/

-- test: incompatible type
CREATE TABLE test(a DOUBLE DEFAULT 'hello');
-- error:

-- test: function
CREATE TABLE test(a DOUBLE DEFAULT pk());
-- error:
-- test: same type
CREATE TABLE test(a INT DEFAULT 10);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER DEFAULT 10)"
}
*/

-- test: compatible type
CREATE TABLE test(a DOUBLE DEFAULT 10);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a DOUBLE DEFAULT 10)"
}
*/

-- test: expr
CREATE TABLE test(a DOUBLE DEFAULT 1 + 4 / 4);
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a DOUBLE DEFAULT 1 + 4 / 4)"
}
*/

-- test: incompatible type
CREATE TABLE test(a DOUBLE DEFAULT 'hello');
-- error:

-- test: function
CREATE TABLE test(a DOUBLE DEFAULT pk());
-- error:

-- test: incompatible expr
CREATE TABLE test(a BLOB DEFAULT 1 + 4 / 4);
-- error:

-- test: forbidden tokens: AND
CREATE TABLE test(a BLOB DEFAULT 1 AND 1);
-- error:

-- test: forbidden tokens: path
CREATE TABLE test(a BLOB DEFAULT b);
-- error:

-- test: DEFAULT nextval sequence
CREATE SEQUENCE seq1;
CREATE TABLE test(a INTEGER DEFAULT nextval('seq1'));
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER DEFAULT nextval(\"seq1\"))"
}
*/

-- test: DEFAULT with parentheses
CREATE TABLE test(a INTEGER DEFAULT (1 + 2));
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER DEFAULT 1 + 2)"
}
*/

-- test: DEFAULT (nextval sequence)
CREATE SEQUENCE seq_paren;
CREATE TABLE test(a INTEGER DEFAULT (nextval('seq_paren')));
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER DEFAULT nextval(\"seq_paren\"))"
}
*/

-- test: DEFAULT with nested parentheses
CREATE TABLE test(a INTEGER DEFAULT ((1)));
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER DEFAULT (1))"
}
*/

