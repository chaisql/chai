-- test: undeclared field
CREATE TABLE test;
CREATE INDEX test_a_idx ON test(a);
-- error:

-- test: undeclared field: IF NOT EXISTS
CREATE TABLE test;
CREATE INDEX IF NOT EXISTS test_a_idx ON test(a);
-- error:

-- test: undeclared field: other fields
CREATE TABLE test(b int);
CREATE INDEX test_a_idx ON test(a);
-- error:

-- test: undeclared field: variadic
CREATE TABLE test(b int, ...);
CREATE INDEX test_a_idx ON test(a);
-- error:
