-- test: read-only tables
INSERT INTO __chai_catalog (name, namespace) VALUES ('foo', 100);
-- error: cannot write to read-only table

-- test: insert with primary keys
CREATE TABLE testpk (foo INTEGER PRIMARY KEY);
INSERT INTO testpk (bar) VALUES (1);
-- error:

-- test: insert with primary keys: duplicate
CREATE TABLE testpk (foo INTEGER PRIMARY KEY);
INSERT INTO testpk (bar, foo) VALUES (1, 2);
INSERT INTO testpk (bar, foo) VALUES (1, 2);
-- error:

-- test: insert with types constraints
CREATE TABLE test_tc(
    b bool, db double,
    i bigint, bb bytea, byt bytes,
    t text primary key
);
INSERT INTO test_tc (i, db, b, bb, byt, t) VALUES (10000000000, 21.21, true, "YmxvYlZhbHVlCg==", "Ynl0ZXNWYWx1ZQ==", "text");
SELECT * FROM test_tc;
/* result:
{
  "b": true,
  "db": 21.21,
  "i": 10000000000,
  "bb": CAST("YmxvYlZhbHVlCg==" AS BYTEA),
  "byt": CAST("Ynl0ZXNWYWx1ZQ==" AS BYTES),
  "t": "text"
}
*/

-- test: insert with inferred constraints
CREATE TABLE test_ic(a INTEGER PRIMARY KEY, s.b TEXT);
INSERT INTO test_ic VALUES {s: 1};
-- error:

-- test: insert with on conflict do nothing, pk
CREATE TABLE test_oc(a INTEGER PRIMARY KEY);
INSERT INTO test_oc (a) VALUES (1) ON CONFLICT DO NOTHING;
INSERT INTO test_oc (a) VALUES (1) ON CONFLICT DO NOTHING;
SELECT * FROM test_oc;
/* result:
{
  a: 1
}
*/

-- test: insert with on conflict do nothing, unique
CREATE TABLE test_oc(pk INTEGER PRIMARY KEY, a INTEGER UNIQUE);
INSERT INTO test_oc (pk, a) VALUES (1, 1) ON CONFLICT DO NOTHING;
INSERT INTO test_oc (pk, a) VALUES (1, 1) ON CONFLICT DO NOTHING;
SELECT a FROM test_oc;
/* result:
{
  a: 1
}
*/

-- test: insert with on conflict do nothing, unique with default
CREATE TABLE test_oc(a INTEGER PRIMARY KEY, b INTEGER UNIQUE DEFAULT 10);
INSERT INTO test_oc (a) VALUES (1) ON CONFLICT DO NOTHING;
INSERT INTO test_oc (a) VALUES (1) ON CONFLICT DO NOTHING;
SELECT * FROM test_oc;
/* result:
{
  a: 1,
  b: 10
}
*/

-- test: insert with on conflict do nothing, overall
CREATE TABLE test_oc(a INTEGER UNIQUE, b INTEGER PRIMARY KEY, c INTEGER UNIQUE DEFAULT 10);
INSERT INTO test_oc (a, b, c) VALUES (1, 1, 1);
INSERT INTO test_oc (a, b, c) VALUES (1, 2, 3) ON CONFLICT DO NOTHING; -- unique constraint
INSERT INTO test_oc (a, b, c) VALUES (2, 1, 4) ON CONFLICT DO NOTHING; -- primary key
INSERT INTO test_oc (a, b, c) VALUES (2, 2, 1) ON CONFLICT DO NOTHING; -- unique constraint
INSERT INTO test_oc (a, b) VALUES (2, 2); -- should insert
INSERT INTO test_oc (a, b) VALUES (3, 3) ON CONFLICT DO NOTHING; -- should not insert
SELECT * FROM test_oc;
/* result:
{
  a: 1,
  b: 1,
  c: 1
}
{
  a: 2,
  b: 2,
  c: 10
}
*/

-- test: insert with on conflict do replace, pk
CREATE TABLE test_oc(a INTEGER PRIMARY KEY, b INTEGER, c INTEGER);
INSERT INTO test_oc (a, b, c) VALUES (1, 1, 1);
INSERT INTO test_oc (a, b, c) VALUES (1, 2, 3) ON CONFLICT DO REPLACE;
SELECT * FROM test_oc;
/* result:
{
  a: 1,
  b: 2,
  c: 3
}
*/

-- test: insert with on conflict do replace, unique
CREATE TABLE test_oc(a INTEGER UNIQUE, b INTEGER PRIMARY KEY, c INTEGER);
INSERT INTO test_oc (a, b, c) VALUES (1, 1, 1);
INSERT INTO test_oc (a, b, c) VALUES (1, 2, 3) ON CONFLICT DO REPLACE;
SELECT * FROM test_oc;
/* result:
{
  a: 1,
  b: 2,
  c: 3
}
*/

-- test: insert with on conflict do replace, not null
CREATE TABLE test_oc(a INTEGER NOT NULL, b INTEGER PRIMARY KEY, c INTEGER);
INSERT INTO test_oc (b, c) VALUES (1, 1) ON CONFLICT DO REPLACE;
-- error:

-- test: insert with nextval
CREATE TABLE test_oc(a INTEGER PRIMARY KEY);
CREATE SEQUENCE test_seq;
INSERT INTO test_oc (a) VALUES (nextval('test_seq'));
INSERT INTO test_oc (a) VALUES (nextval('test_seq')), (nextval('test_seq'));
SELECT * FROM test_oc;
/* result:
{
  a: 1
}
{
  a: 2
}
{
  a: 3
}
*/

-- test: duplicate column names: root
CREATE TABLE test_df(a INT PRIMARY KEY);
INSERT INTO test_df(a, a) VALUES (1, 10);
-- error:

-- test: inserts must be silent
CREATE TABLE test (a int PRIMARY KEY);
INSERT INTO test VALUES (1);
/* result:
*/

-- test: inserts must be silent: explain
CREATE TABLE test (a int PRIMARY KEY);
EXPLAIN INSERT INTO test (a) VALUES (1);
/* result:
{plan: "rows.Emit((1)) | table.Validate(\"test\") | table.GenerateKey(\"test\") | table.Insert(\"test\") | discard()"}
*/

-- test: with columns
CREATE TABLE test(a int PRIMARY KEY, b text);
INSERT INTO test(a, b) VALUES (1, 'a');
SELECT * FROM test;
/* result:
{
  a: 1,
  b: "a"
}
*/