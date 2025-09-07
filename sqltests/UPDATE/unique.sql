-- setup:
CREATE TABLE test(pk int primary key, a int UNIQUE);

-- test: conflict
INSERT INTO test VALUES (1, 1), (2, 2);
UPDATE test SET a = 2 WHERE a = 1;
-- error: UNIQUE constraint error: [a]