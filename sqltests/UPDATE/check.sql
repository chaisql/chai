-- test: int type constraint, double
CREATE TABLE test (pk int primary key, a int CHECK(a > 10));
INSERT INTO test (pk, a) VALUES (11, 11);
UPDATE test SET a = 15.2;
SELECT a FROM test;
/* result:
{
    a: 15
}
*/
