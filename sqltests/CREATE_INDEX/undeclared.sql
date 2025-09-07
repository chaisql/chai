-- test: undeclared column
CREATE TABLE test (a int primary key);
CREATE INDEX test_a_idx ON test(a);
-- error:

-- test: undeclared column: IF NOT EXISTS
CREATE TABLE test (a int primary key);
CREATE INDEX IF NOT EXISTS test_a_idx ON test(a);
-- error:

-- test: undeclared column: other columns
CREATE TABLE test(b int primary key);
CREATE INDEX test_a_idx ON test(a);
-- error:
