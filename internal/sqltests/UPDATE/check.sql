-- test: no type constraint, valid double
CREATE TABLE test (a CHECK(a > 10));
INSERT INTO test (a) VALUES (11);
UPDATE test SET a = 12;
SELECT * FROM test;
/* result:
{
    a: 12.0
}
*/

-- test: no type constraint, invalid double
CREATE TABLE test (a CHECK(a > 10));
INSERT INTO test (a) VALUES (11);
UPDATE test SET a = 1;
-- error: row violates check constraint "test_check"

-- test: no type constraint, text
CREATE TABLE test (a CHECK(a > 10));
INSERT INTO test (a) VALUES (11);
UPDATE test SET a = "hello";
-- error: row violates check constraint "test_check"

-- test: no type constraint, null
CREATE TABLE test (a CHECK(a > 10));
INSERT INTO test (a) VALUES (11);
UPDATE test UNSET a;
SELECT * FROM test;
/* result:
{}
*/

-- test: int type constraint, double
CREATE TABLE test (a int CHECK(a > 10));
INSERT INTO test (a) VALUES (11);
UPDATE test SET a = 15.2;
SELECT * FROM test;
/* result:
{
    a: 15
}
*/
