/*
Check behavior: These tests check the behavior of the check constraint depending
on the result of the evaluation of the expression.
*/

-- test: boolean check constraint
CREATE TABLE test (a text PRIMARY KEY CHECK(true));
INSERT INTO test (a) VALUES ('hello');
SELECT * FROM test;
/* result:
{
    a: 'hello'
}
*/

-- test: non-boolean check constraint, numeric result
CREATE TABLE test (a text PRIMARY KEY CHECK(1 + 1));
INSERT INTO test (a) VALUES ('hello');
SELECT * FROM test;
/* result:
{
    a: 'hello'
}
*/

-- test: non-boolean check constraint
CREATE TABLE test (a text PRIMARY KEY CHECK('hello'));
INSERT INTO test (a) VALUES ('hello');
-- error: row violates check constraint "test_check"

-- test: non-boolean check constraint, NULL
CREATE TABLE test (a text PRIMARY KEY CHECK(NULL));
INSERT INTO test (a) VALUES ('hello');
SELECT * FROM test;
/* result:
{
    a: 'hello'
}
*/

/*
Column types: These tests check the behavior of the check constraint depending
on the type of the column
*/

-- test: valid int
CREATE TABLE test (a INT PRIMARY KEY CHECK(a > 10));
INSERT INTO test (a) VALUES (11);
SELECT * FROM test;
/* result:
{
    a: 11
}
*/

-- test: invalid int
CREATE TABLE test (a INT PRIMARY KEY CHECK(a > 10));
INSERT INTO test (a) VALUES (1);
-- error: row violates check constraint "test_check"

-- test: multiple checks, invalid int
CREATE TABLE test (a INT PRIMARY KEY CHECK(a > 10), CHECK(a < 20));
INSERT INTO test (a) VALUES (40);
-- error: row violates check constraint "test_check1"

-- test: text
CREATE TABLE test (a INT PRIMARY KEY CHECK(a > 10));
INSERT INTO test (a) VALUES ('hello');
-- error: cannot cast "hello" as integer: strconv.ParseInt: parsing "hello": invalid syntax

-- test: null
CREATE TABLE test (a INT CHECK(a > 10), b int PRIMARY KEY);
INSERT INTO test (b) VALUES (10);
SELECT b FROM test;
/* result:
{
    b: 10
}
*/

-- test: double
CREATE TABLE test (a int PRIMARY KEY CHECK(a > 10));
INSERT INTO test (a) VALUES (15.2);
SELECT * FROM test;
/* result:
{
    a: 15
}
*/
