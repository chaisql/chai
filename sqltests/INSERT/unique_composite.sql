-- setup:
CREATE TABLE test (pk int primary key, a int, b int, c int, d int, UNIQUE (a, b, c));

-- test: same value
INSERT INTO test (pk, a, b, c, d) VALUES (1, 1, 1, 1, 1); 
INSERT INTO test (pk, a, b, c, d) VALUES (2, 1, 1, 1, 1); 
-- error:

-- test: same value, same statement
INSERT INTO test (pk, a, b, c, d) VALUES (1, 1, 1, 1, 1), (2, 1, 1, 1, 1); 
-- error:

-- test: different values
INSERT INTO test (pk, a, b, c, d) VALUES (1, 1, 1, 1, 1), (2, 1, 2, 1, 1);
SELECT a, b, c, d FROM test;
/* result:
{a: 1, b: 1, c: 1, d: 1}
{a: 1, b: 2, c: 1, d: 1}
*/

-- test: NULL
INSERT INTO test (pk, d) VALUES (1, 1), (2, 2);
INSERT INTO test (pk, c, d) VALUES (3, 3, 3);
INSERT INTO test (pk, c, d) VALUES (4, 3, 3);
INSERT INTO test (pk, b, c, d) VALUES (5, 4, 4, 4);
INSERT INTO test (pk, b, c, d) VALUES (6, 4, 4, 4);
INSERT INTO test (pk, a, b, c, d) VALUES (7, 5, null, 5, 5);
INSERT INTO test (pk, a,  c, d) VALUES (8, 5, 5, 5);
SELECT a, b, c, d FROM test;
/* result:
{a: NULL, b: NULL, c: NULL, d: 1}
{a: NULL, b: NULL, c: NULL, d: 2}
{a: NULL, b: NULL, c: 3, d: 3}
{a: NULL, b: NULL, c: 3, d: 3}
{a: NULL, b: 4, c: 4, d: 4}
{a: NULL, b: 4, c: 4, d: 4}
{a: 5, b: NULL, c: 5, d: 5}
{a: 5, b: NULL, c: 5, d: 5}
*/
