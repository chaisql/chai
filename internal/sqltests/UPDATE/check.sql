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
