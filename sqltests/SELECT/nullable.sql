-- test: one nullable column
CREATE TABLE test (pk INT PRIMARY KEY, a INT);
INSERT INTO test (pk, a) VALUES (1, null), (2, null);
SELECT a FROM test;
/* result:
{
    a: null
}
{
    a: null
}
*/

-- test: first column null
CREATE TABLE test (pk INT PRIMARY KEY, a INT, b INT);
INSERT INTO test (pk, b) VALUES (1, 1), (2, 2);
SELECT a, b FROM test;
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
CREATE TABLE test (pk INT PRIMARY KEY, a INT, b INT);
INSERT INTO test (pk, a) VALUES (1, 1), (2, 2);
SELECT a, b FROM test;
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
