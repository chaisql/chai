-- test: VALUES RETURNING *
CREATE TABLE test (a INT PRIMARY KEY, b INT);
INSERT INTO test (a, b) VALUES (1, 2) RETURNING *;
/* result:
{
  "a": 1,
  "b": 2
}
*/

-- test: VALUES RETURNING with alias and explicit columns
CREATE TABLE test (a INT PRIMARY KEY, b INT);
INSERT INTO test (a, b) VALUES (3, 4) RETURNING a, b as B;
/* result:
{
  "a": 3,
  "B": 4
}
*/

-- test: INSERT ... SELECT RETURNING a
CREATE TABLE foo (c INT PRIMARY KEY, d INT);
INSERT INTO foo (c, d) VALUES (10, 20);
CREATE TABLE test2 (a INT PRIMARY KEY, b INT);
INSERT INTO test2 (a, b) SELECT c, d FROM foo RETURNING a;
/* result:
{
  "a": 10
}
*/

-- test: INSERT ... SELECT RETURNING * with multiple rows
CREATE TABLE foo2 (c INT PRIMARY KEY, d INT);
INSERT INTO foo2 (c, d) VALUES (1, 2), (3, 4);
CREATE TABLE test3 (a INT PRIMARY KEY, b INT);
INSERT INTO test3 (a, b) SELECT c, d FROM foo2 RETURNING *;
/* result:
{ "a": 1, "b": 2 }
{ "a": 3, "b": 4 }
*/
