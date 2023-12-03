-- setup:
CREATE TABLE test(a int);

-- test: field constraints are updated
INSERT INTO test VALUES (1), (2);
ALTER TABLE test ADD COLUMN b int DEFAULT 0;
SELECT name, sql FROM __chai_catalog WHERE type = "table" AND name = "test";
/* result:
{
  "name": "test",
  "sql": "CREATE TABLE test (a INTEGER, b INTEGER DEFAULT 0)"
}
*/

-- test: default value is updated
INSERT INTO test VALUES (1), (2);
ALTER TABLE test ADD COLUMN b int DEFAULT 0;
SELECT * FROM test;
/* result:
{
  "a": 1,
  "b": 0
}
{
  "a": 2,
  "b": 0
}
*/

-- test: not null alone
INSERT INTO test VALUES (1), (2);
ALTER TABLE test ADD COLUMN b int NOT NULL;
-- error: NOT NULL constraint error: [b]

-- test: not null with default
INSERT INTO test VALUES (1), (2);
ALTER TABLE test ADD COLUMN b int NOT NULL DEFAULT 10;
SELECT * FROM test;
/* result:
{
  "a": 1,
  "b": 10
}
{
  "a": 2,
  "b": 10
}
*/

-- test: unique
INSERT INTO test VALUES (1), (2);
ALTER TABLE test ADD COLUMN b int UNIQUE;
SELECT * FROM test;
/* result:
{
  "a": 1
}
{
  "a": 2
}
*/

-- test: unique with default: with data
INSERT INTO test VALUES (1), (2);
ALTER TABLE test ADD COLUMN b int UNIQUE DEFAULT 10;
-- error: UNIQUE constraint error: [b]

-- test: unique with default: without data
ALTER TABLE test ADD COLUMN b int UNIQUE DEFAULT 10;
INSERT INTO test VALUES (1), (2);
-- error: UNIQUE constraint error: [b]

-- test: primary key: with data
INSERT INTO test VALUES (1), (2);
ALTER TABLE test ADD COLUMN b int PRIMARY KEY;
-- error: NOT NULL constraint error: [b]

-- test: primary key: without data
ALTER TABLE test ADD COLUMN b int PRIMARY KEY;
INSERT INTO test VALUES (1, 10), (2, 20);
SELECT pk() FROM test;
/* result:
{
  "pk()": [10]
}
{
  "pk()": [20]
}
*/

-- test: primary key: with default: with data
INSERT INTO test VALUES (1), (2);
ALTER TABLE test ADD COLUMN b int PRIMARY KEY DEFAULT 10;
-- error: PRIMARY KEY constraint error: [b]

-- test: primary key: with default: without data
ALTER TABLE test ADD COLUMN b int PRIMARY KEY DEFAULT 10;
INSERT INTO test VALUES (2, 20), (3, 30);
INSERT INTO test (a) VALUES (1);
SELECT * FROM test;
/* result:
{
  "a": 1,
  "b": 10
}
{
  "a": 2,
  "b": 20
}
{
  "a": 3,
  "b": 30
}
*/

-- test: no type
INSERT INTO test VALUES (1), (2);
ALTER TABLE test ADD COLUMN b;
INSERT INTO test VALUES (3, 30), (4, 'hello');
SELECT * FROM test;
/* result:
{
  "a": 1
}
{
  "a": 2
}
{
  "a": 3,
  "b": 30.0
}
{
  "a": 4,
  "b": "hello"
}
*/

-- test: bad syntax: no field name
ALTER TABLE test ADD COLUMN;
-- error:

-- test: bad syntax: missing FIELD keyword
ALTER TABLE test ADD a int;
-- error: