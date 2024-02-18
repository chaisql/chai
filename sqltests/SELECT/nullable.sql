-- test: one nullable column
CREATE TABLE test (a INT);
INSERT INTO test (a) VALUES (null), (null);
SELECT * FROM test;
/* result:
{
    a: null
}
{
    a: null
}
*/

-- test: first column null
CREATE TABLE test (a INT, b INT);
INSERT INTO test (b) VALUES (1), (2);
SELECT * FROM test;
/* result:
{
    a: null,
    b: 1
}
{
    a: null,
    b: 2
}
*/

-- test: second column null
CREATE TABLE test (a INT, b INT);
INSERT INTO test (a) VALUES (1), (2);
SELECT * FROM test;
/* result:
{
    a: 1,
    b: null
}
{
    a: 2,
    b: null
}
*/
