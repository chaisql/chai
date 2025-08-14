-- setup:
CREATE TABLE test(a int UNIQUE);

-- test: conflict
INSERT INTO test VALUES (1), (2);
UPDATE test SET a = 2 WHERE a = 1;
-- error: UNIQUE constraint error: [a]