-- test: insert with errors, not null without type constraint
CREATE TABLE test_e (a NOT NULL);
INSERT INTO test_e VALUES {};
-- error:

-- test: insert with errors, array / not null with type constraint
CREATE TABLE test_e (a ARRAY NOT NULL);
INSERT INTO test_e VALUES {};
-- error:

-- test: insert with errors, array / not null with non-respected type constraint
CREATE TABLE test_e (a ARRAY NOT NULL);
INSERT INTO test_e VALUES {a: 42};
-- error:

-- test: insert with errors, blob
CREATE TABLE test_e (a BLOB);
INSERT INTO test_e {a: true};
-- error:

-- test: blob / not null with type constraint
CREATE TABLE test_e (a BLOB NOT NULL);
INSERT INTO test_e {};
-- error:

-- test: blob / not null with non-respected type constraint
CREATE TABLE test_e (a BLOB NOT NULL);
INSERT INTO test_e {a: 42};
-- error:

-- test: bool / not null with type constraint
CREATE TABLE test_e (a BOOL NOT NULL);
INSERT INTO test_e {};
-- error:

-- test: bytes
CREATE TABLE test_e (a BYTES);
INSERT INTO test_e {a: [1,2,3]};
-- error:

-- test: bytes / not null with type constraint
CREATE TABLE test_e (a BYTES NOT NULL);
INSERT INTO test_e {};
-- error:

-- test: bytes / not null with non-respected type constraint
CREATE TABLE test_e (a BYTES NOT NULL);
INSERT INTO test_e {a: 42};
-- error:

-- test: document
CREATE TABLE test_e (a DOCUMENT);
INSERT INTO test_e {"a": "foo"};
-- error:

-- test: document / not null with type constraint
CREATE TABLE test_e (a DOCUMENT NOT NULL);
INSERT INTO test_e {};
-- error:

-- test: document / not null with non-respected type constraint
CREATE TABLE test_e (a DOCUMENT NOT NULL);
INSERT INTO test_e {a: false};
-- error:

-- test: double
CREATE TABLE test_e (a DOUBLE);
INSERT INTO test_e {a: "foo"};
-- error:

-- test: double / not null with type constraint
CREATE TABLE test_e (a DOUBLE NOT NULL);
INSERT INTO test_e {};
-- error:

-- test: double / not null with non-respected type constraint
CREATE TABLE test_e (a DOUBLE NOT NULL);
INSERT INTO test_e {a: [1,2,3]};
-- error:

-- test: double precision
CREATE TABLE test_e (a DOUBLE PRECISION);
INSERT INTO test_e {a: "foo"};
-- error:

-- test: double precision / not null with type constraint
CREATE TABLE test_e (a DOUBLE PRECISION NOT NULL);
INSERT INTO test_e {};
-- error:

-- test: double precision / not null with non-respected type constraint
CREATE TABLE test_e (a DOUBLE PRECISION NOT NULL);
INSERT INTO test_e {a: [1,2,3]};
-- error:

-- test: real
CREATE TABLE test_e (a REAL);
INSERT INTO test_e {a: "foo"};
-- error:

-- test: real / not null with type constraint
CREATE TABLE test_e (a REAL NOT NULL);
INSERT INTO test_e {};
-- error:

-- test: real / not null with non-respected type constraint
CREATE TABLE test_e (a REAL NOT NULL);
INSERT INTO test_e {a: [1,2,3]};
-- error:

-- test: integer
CREATE TABLE test_e (a INTEGER);
INSERT INTO test_e {a: "foo"};
-- error:

-- test: integer / not null with type constraint
CREATE TABLE test_e (a INTEGER NOT NULL);
INSERT INTO test_e {};
-- error:

-- test: integer / not null with non-respected type constraint
CREATE TABLE test_e (a INTEGER NOT NULL);
INSERT INTO test_e {a: [1,2,3]};
-- error:

-- test: int2
CREATE TABLE test_e (a INT2);
INSERT INTO test_e {a: "foo"};
-- error:

-- test: int2 / not null with type constraint
CREATE TABLE test_e (a INT2 NOT NULL);
INSERT INTO test_e {};
-- error:

-- test: int2 / not null with non-respected type constraint
CREATE TABLE test_e (a INT NOT NULL);
INSERT INTO test_e {a: [1,2,3]};
-- error:

-- test: int8
CREATE TABLE test_e (a INT8);
INSERT INTO test_e {a: "foo"};
-- error:

-- test: int8 / not null with type constraint
CREATE TABLE test_e (a INT8 NOT NULL);
INSERT INTO test_e {};
-- error:

-- test: int8 / not null with non-respected type constraint
CREATE TABLE test_e (a INT8 NOT NULL);
INSERT INTO test_e {a: [1,2,3]};
-- error:

-- test: tinyint
CREATE TABLE test_e (a TINYINT);
INSERT INTO test_e {a: "foo"};
-- error:

-- test: tinyint / not null with type constraint
CREATE TABLE test_e (a TINYINT NOT NULL);
INSERT INTO test_e {};
-- error:

-- test: tinyint / not null with non-respected type constraint
CREATE TABLE test_e (a TINYINT NOT NULL);
INSERT INTO test_e {a: [1,2,3]};
-- error:

-- test: bigint
CREATE TABLE test_e (a BIGINT);
INSERT INTO test_e {a: "foo"};
-- error:

-- test: bigint / not null with type constraint
CREATE TABLE test_e (a BIGINT NOT NULL);
INSERT INTO test_e {};
-- error:

-- test: bigint / not null with non-respected type constraint
CREATE TABLE test_e (a BIGINT NOT NULL);
INSERT INTO test_e {a: [1,2,3]};
-- error:

-- test: smallint
CREATE TABLE test_e (a SMALLINT);
INSERT INTO test_e {a: "foo"};
-- error:

-- test: smallint / not null with type constraint
CREATE TABLE test_e (a SMALLINT NOT NULL);
INSERT INTO test_e {};
-- error:

-- test: smallint / not null with non-respected type constraint
CREATE TABLE test_e (a SMALLINT NOT NULL);
INSERT INTO test_e {a: [1,2,3]};
-- error:

-- test: mediumint
CREATE TABLE test_e (a MEDIUMINT);
INSERT INTO test_e {a: "foo"};
-- error:

-- test: mediumint / not null with type constraint
CREATE TABLE test_e (a MEDIUMINT NOT NULL);
INSERT INTO test_e {};
-- error:

-- test: mediumint / not null with non-respected type constraint
CREATE TABLE test_e (a MEDIUMINT NOT NULL);
INSERT INTO test_e {a: [1,2,3]};
-- error:

-- test: text / not null with type constraint
CREATE TABLE test_e (a TEXT NOT NULL);
INSERT INTO test_e {};
-- error:

-- test: varchar / not null with type constraint
CREATE TABLE test_e (a VARCHAR(255) NOT NULL);
INSERT INTO test_e {};
-- error:

-- test: character / not null with type constraint
CREATE TABLE test_e (a CHARACTER(64) NOT NULL);
INSERT INTO test_e {};
-- error:
