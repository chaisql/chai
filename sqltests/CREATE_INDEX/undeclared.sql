-- test: undeclared column
CREATE TABLE test;
CREATE INDEX test_a_idx ON test(a);
-- error:

-- test: undeclared column: IF NOT EXISTS
CREATE TABLE test;
CREATE INDEX IF NOT EXISTS test_a_idx ON test(a);
-- error:

-- test: undeclared column: other columns
CREATE TABLE test(b int);
CREATE INDEX test_a_idx ON test(a);
-- error:
