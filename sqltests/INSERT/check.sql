-- test: no type constraint, valid double
CREATE TABLE test (a CHECK(a > 10));
INSERT INTO test (a) VALUES (11);
SELECT * FROM test;
/* result:
{
    a: 11.0
}
*/

-- test: no type constraint, invalid double
CREATE TABLE test (a CHECK(a > 10));
INSERT INTO test (a) VALUES (1);
-- error: document violates check constraint "test_check"

-- test: no type constraint, multiple checks, invalid double
CREATE TABLE test (a CHECK(a > 10), CHECK(a < 20));
INSERT INTO test (a) VALUES (40);
-- error: document violates check constraint "test_check1"

-- test: no type constraint, text
CREATE TABLE test (a CHECK(a > 10));
INSERT INTO test (a) VALUES ("hello");
SELECT * FROM test;
/* result:
{
    a: "hello"
}
*/

-- test: no type constraint, null
CREATE TABLE test (a CHECK(a > 10));
INSERT INTO test (b) VALUES (10);
SELECT * FROM test;
/* result:
{
    b: 10.0
}
*/

-- test: int type constraint, double
CREATE TABLE test (a int CHECK(a > 10));
INSERT INTO test (a) VALUES (15.2);
SELECT * FROM test;
/* result:
{
    a: 15
}
*/
