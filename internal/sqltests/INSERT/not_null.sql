-- test: enforced type
CREATE TABLE test (a INT NOT NULL, b INT);
INSERT INTO test (b) VALUES (1);
-- error:

-- test: non-enforced type
CREATE TABLE test (a NOT NULL, b INT);
INSERT INTO test (b) VALUES (1);
-- error:

-- test: with null
CREATE TABLE test (a INT NOT NULL, b INT);
INSERT INTO test (a, b) VALUES (NULL, 1);
-- error:

-- test: with missing field and default
CREATE TABLE test (a INT NOT NULL DEFAULT 10, b INT);
INSERT INTO test (b) VALUES (1);
SELECT a, b FROM test;
/* result:
{
  "a": 10,
  "b": 1
}
*/

-- test: with null and default should fail
CREATE TABLE test (a INT NOT NULL DEFAULT 10, b INT);
INSERT INTO test (a, b) VALUES (NULL, 1);
-- error:
