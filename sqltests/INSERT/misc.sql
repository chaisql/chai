-- test: read-only tables
INSERT INTO __genji_catalog VALUES {a: 400, b: a * 4};
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

-- test: insert with shadowing
CREATE TABLE test (`pk()` INT);
INSERT INTO test (`pk()`) VALUES (10);
SELECT pk() AS pk, `pk()` from test;
/* result:
{
  "pk": [1],
  "pk()": 10
}
*/

-- test: insert with types constraints
CREATE TABLE test_tc(
    b bool, db double,
    i integer, bb blob, byt bytes,
    t text, a array, d object
);

INSERT INTO test_tc
VALUES {
    i: 10000000000, db: 21.21, b: true,
    bb: "YmxvYlZhbHVlCg==", byt: "Ynl0ZXNWYWx1ZQ==",
    t: "text", a: [1, "foo", true], d: {"foo": "bar"}
};

SELECT * FROM test_tc;
/* result:
{
  "b": true,
  "db": 21.21,
  "i": 10000000000,
  "bb": CAST("YmxvYlZhbHVlCg==" AS BLOB),
  "byt": CAST("Ynl0ZXNWYWx1ZQ==" AS BYTES),
  "t": "text",
  "a": [
    1.0,
    "foo",
    true
  ],
  "d": {
    "foo": "bar"
  }
}
*/

-- test: insert with inferred constraints
CREATE TABLE test_ic(a INTEGER, s.b TEXT);
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
CREATE TABLE test_oc(a INTEGER UNIQUE);
INSERT INTO test_oc (a) VALUES (1) ON CONFLICT DO NOTHING;
INSERT INTO test_oc (a) VALUES (1) ON CONFLICT DO NOTHING;
SELECT * FROM test_oc;
/* result:
{
  a: 1
}
*/

-- test: insert with on conflict do nothing, unique with default
CREATE TABLE test_oc(a INTEGER, b INTEGER UNIQUE DEFAULT 10);
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
CREATE TABLE test_oc(a INTEGER PRIMARY KEY, ...);
INSERT INTO test_oc (a, b, c) VALUES (1, 1, 1);
INSERT INTO test_oc (a, b, c) VALUES (1, 2, 3) ON CONFLICT DO REPLACE;
SELECT * FROM test_oc;
/* result:
{
  a: 1,
  b: 2.0,
  c: 3.0
}
*/

-- test: insert with on conflict do replace, unique
CREATE TABLE test_oc(a INTEGER UNIQUE, ...);
INSERT INTO test_oc (a, b, c) VALUES (1, 1, 1);
INSERT INTO test_oc (a, b, c) VALUES (1, 2, 3) ON CONFLICT DO REPLACE;
SELECT * FROM test_oc;
/* result:
{
  a: 1,
  b: 2.0,
  c: 3.0
}
*/

-- test: insert with on conflict do replace, not null
CREATE TABLE test_oc(a INTEGER NOT NULL, ...);
INSERT INTO test_oc (b, c) VALUES (1, 1) ON CONFLICT DO REPLACE;
-- error:

-- test: insert with NEXT VALUE FOR
CREATE TABLE test_oc(a INTEGER UNIQUE);
CREATE SEQUENCE test_seq;
INSERT INTO test_oc (a) VALUES (NEXT VALUE FOR test_seq);
INSERT INTO test_oc (a) VALUES (NEXT VALUE FOR test_seq), (NEXT VALUE FOR test_seq);
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

-- test: default on nested fields 
CREATE TABLE test_df (a (b TEXT DEFAULT "foo"));
INSERT INTO test_df VALUES {};
SELECT * FROM test_df;
/* result:
{
}
*/

-- test: duplicate field names: root
CREATE TABLE test_df;
INSERT INTO test_df(a, a) VALUES (1, 10);
-- error:

-- test: duplicate field names: nested
CREATE TABLE test_df;
insert into test_df(a) values ({b: 1, b: 10});
-- error:

-- test: inserts must be silent
CREATE TABLE test (a int);
INSERT INTO test VALUES (1);
/* result:
*/

-- test: inserts must be silent: explain
CREATE TABLE test (a int);
EXPLAIN INSERT INTO test (a) VALUES (1);
/* result:
{plan: "rows.Emit({a: 1}) | table.Validate(\"test\") | table.Insert(\"test\") | discard()"}
*/