-- test: set primary key
CREATE TABLE test (a int primary key, b int);
INSERT INTO test (a, b) VALUES (1, 10); 
UPDATE test SET a = 2, b = 20 WHERE a = 1;
INSERT INTO test (a, b) VALUES (1, 10);
SELECT * FROM test;
/* result:
{a: 1, b: 10}
{a: 2, b: 20}
*/

-- test: set primary key / conflict
CREATE TABLE test (a int primary key, b int);
INSERT INTO test (a, b) VALUES (1, 10), (2, 20);
UPDATE test SET a = 2, b = 20 WHERE a = 1;
-- error: PRIMARY KEY constraint error: [a]

-- test: set composite primary key
CREATE TABLE test (a int, b int, c int, PRIMARY KEY(a, b));
INSERT INTO test (a, b, c) VALUES (1, 10, 100); 
UPDATE test SET a = 2, b = 20 WHERE a = 1;
INSERT INTO test (a, b, c) VALUES (1, 10, 100);
SELECT * FROM test;
/* result:
{a: 1, b: 10, c: 100}
{a: 2, b: 20, c: 100}
*/
