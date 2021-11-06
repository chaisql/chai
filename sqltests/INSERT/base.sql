-- setup:
CREATE TABLE test;
CREATE TABLE test_idx;

CREATE INDEX idx_a ON test_idx (a);
CREATE INDEX idx_b ON test_idx (b);
CREATE INDEX idx_c ON test_idx (c);

-- test: values, no columns
INSERT INTO test VALUES ("a", 'b', 'c');
-- error:

-- test: values, with columns
INSERT INTO test (a, b, c) VALUES ('a', 'b', 'c');
SELECT pk(), * FROM test;
/* result:
{
  "pk()": 1,
  "a": "a",
  "b": "b",
  "c":"c"
}
*/

-- test: values, ident
INSERT INTO test (a) VALUES (a);
-- error: field not found

-- test: values, ident string
INSERT INTO test (a) VALUES (`a`);
-- error: field not found

-- test: values, fields ident string
INSERT INTO test (a, `foo bar`) VALUES ('c', 'd');
SELECT pk(), * FROM test;
/* result:
{
  "pk()": 1,
  "a": "c",
  "foo bar": "d"
}
*/

-- test: values, list
INSERT INTO test (a, b, c) VALUES ("a", 'b', [1, 2, 3]);
SELECT pk(), * FROM test;
/* result:
{
  "pk()": 1,
  "a": "a",
  "b":"b",
  "c": [1.0, 2.0, 3.0]
}
*/

-- test: values, document
INSERT INTO test (a, b, c) VALUES ("a", 'b', {c: 1, d: c + 1});
SELECT pk(), * FROM test;
/* result:
{
  "pk()": 1,
  "a": "a",
  "b": "b",
  "c": {
    "c": 1.0,
    "d": 2.0
  }
}
*/

-- test: document
INSERT INTO test VALUES {a: 'a', b: 2.3, c: 1 = 1};
SELECT pk(), * FROM test;
/* result:
{
  "pk()": 1,
  "a": "a",
  "b": 2.3,
  "c": true
}
*/

-- test: document, list
INSERT INTO test VALUES {a: [1, 2, 3]};
SELECT pk(), * FROM test;
/* result:
{
  "pk()": 1,
  "a": [
    1.0,
    2.0,
    3.0
  ]
}
*/

-- test: document, strings
INSERT INTO test VALUES {'a': 'a', b: 2.3};
SELECT pk(), * FROM test;
/* result:
{
  "pk()": 1,
  "a": "a",
  "b": 2.3
}
*/

-- test: document, double quotes
INSERT INTO test VALUES {"a": "b"};
SELECT pk(), * FROM test;
/* result:
{
  "pk()": 1,
  "a": "b"
}
*/

-- test: document, references to other field
INSERT INTO test VALUES {a: 400, b: a * 4};
SELECT pk(), * FROM test;
/* result:
{"pk()":1,"a":400.0,"b":1600.0}
*/

-- with indexes
-- test: index, values, no columns
INSERT INTO test_idx VALUES ("a", 'b', 'c');
-- error:

-- test: index, values, with columns
INSERT INTO test_idx (a, b, c) VALUES ('a', 'b', 'c');
SELECT pk(), * FROM test_idx;
/* result:
{
  "pk()": 1,
  "a": "a",
  "b": "b",
  "c": "c"
}
*/

-- test: index, values, ident
INSERT INTO test_idx (a) VALUES (a);
-- error: field not found

-- test: index, values, ident string
INSERT INTO test_idx (a) VALUES (`a`);
-- error: field not found

-- test: index, values, fields ident string
INSERT INTO test_idx (a, `foo bar`) VALUES ('c', 'd');
SELECT pk(), * FROM test_idx;
/* result:
{
  "pk()": 1,
  "a": "c",
  "foo bar": "d"
}
*/

-- test: index, values, list
INSERT INTO test_idx (a, b, c) VALUES ("a", 'b', [1, 2, 3]);
SELECT pk(), * FROM test_idx;
/* result:
{
  "pk()": 1,
  "a": "a",
  "b":"b",
  "c": [1.0, 2.0, 3.0]
}
*/

-- test: index, values, document
INSERT INTO test_idx (a, b, c) VALUES ("a", 'b', {c: 1, d: c + 1});
SELECT pk(), * FROM test_idx;
/* result:
{
  "pk()": 1,
  "a": "a",
  "b": "b",
  "c": {
    "c": 1.0,
    "d": 2.0
  }
}
*/

-- test: index, document
INSERT INTO test_idx VALUES {a: 'a', b: 2.3, c: 1 = 1};
SELECT pk(), * FROM test_idx;
/* result:
{
  "pk()": 1,
  "a": "a",
  "b": 2.3,
  "c": true
}
*/

-- test: index, document, list
INSERT INTO test_idx VALUES {a: [1, 2, 3]};
SELECT pk(), * FROM test_idx;
/* result:
{
  "pk()": 1,
  "a": [
    1.0,
    2.0,
    3.0
  ]
}
*/

-- test: index, document, strings
INSERT INTO test_idx VALUES {'a': 'a', b: 2.3};
SELECT pk(), * FROM test_idx;
/*result:
{
  "pk()": 1,
  "a": "a",
  "b": 2.3
}
*/

-- test: index, document, double quotes
INSERT INTO test_idx VALUES {"a": "b"};
SELECT pk(), * FROM test_idx;
/* result:
{
  "pk()": 1,
  "a": "b"
}
*/

-- test: index, document, references to other field
INSERT INTO test_idx VALUES {a: 400, b: a * 4};
SELECT pk(), * FROM test_idx;
/* result:
{"pk()":1,"a":400.0,"b":1600.0}
*/

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
-- error: duplicate

-- test: insert with shadowing
INSERT INTO test (`pk()`) VALUES (10);
SELECT pk() AS pk, `pk()` from test;
/* result:
{
  "pk": 1,
  "pk()": 10.0
}
*/

-- test: insert with types constraints
CREATE TABLE test_tc(
    b bool, db double,
    i integer, bb blob, byt bytes,
    t text, a array, d document
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
  "i": 10000000000,
  "db": 21.21,
  "b": true,
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

-- test: insert with on conflict do nothing
CREATE TABLE test_oc(a INTEGER UNIQUE, b INTEGER PRIMARY KEY, c INTEGER UNIQUE DEFAULT 10);
INSERT INTO test_oc (a, b, c) VALUES (1, 1, 1);
INSERT INTO test_oc (a, b, c) VALUES (1, 2, 3) ON CONFLICT DO NOTHING;
INSERT INTO test_oc (a, b, c) VALUES (2, 1, 4) ON CONFLICT DO NOTHING;
INSERT INTO test_oc (a, b, c) VALUES (2, 2, 1) ON CONFLICT DO NOTHING;
INSERT INTO test_oc (a, b) VALUES (2, 2) ON CONFLICT DO NOTHING;
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
CREATE TABLE test_oc(a INTEGER PRIMARY KEY);
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
CREATE TABLE test_oc(a INTEGER UNIQUE);
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
CREATE TABLE test_oc(a INTEGER NOT NULL);
INSERT INTO test_oc (b, c) VALUES (1, 1) ON CONFLICT DO REPLACE;
-- error:

-- test: insert with NEXT VALUE FOR
CREATE TABLE test_oc(a INTEGER UNIQUE);
CREATE SEQUENCE test_seq1;
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
CREATE TABLE test_df (a.b TEXT DEFAULT "foo");
INSERT INTO test_df VALUES {};
SELECT * FROM test_df;
/* result:
{
  a: {b: "foo"}
}
*/

-- test: default on array indexes 
CREATE TABLE test_df (a.b[0].c TEXT DEFAULT "foo");
INSERT INTO test_df VALUES {};
-- error:
