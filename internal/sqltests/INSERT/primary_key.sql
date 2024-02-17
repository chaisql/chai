-- test: Should fail if Pk not found
CREATE TABLE test (a PRIMARY KEY, b INT);
INSERT INTO test (b) VALUES (1);
-- error:

-- test: Should fail if Pk NULL
CREATE TABLE test (a PRIMARY KEY, b INT);
INSERT INTO test (a, b) VALUES (NULL, 1);
-- error:
