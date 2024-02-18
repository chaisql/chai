-- setup:
CREATE TABLE test (a int unique, b int);

-- test: same value
INSERT INTO test (a, b) VALUES (1, 1); 
INSERT INTO test (a, b) VALUES (1, 1);
-- error:

-- test: same value, same statement
INSERT INTO test (a, b) VALUES (1, 1), (1, 1);
-- error:

-- test: different values
INSERT INTO test (a, b) VALUES (1, 1), (2, 2);
SELECT * FROM test;
/* result:
{a: 1, b: 1}
{a: 2, b: 2}
*/

-- test: NULL
INSERT INTO test (b) VALUES (1), (2);
INSERT INTO test (a, b) VALUES (NULL, 3);
SELECT a, b FROM test;
/* result:
{a: NULL, b: 1}
{a: NULL, b: 2}
{a: NULL, b: 3}
*/
