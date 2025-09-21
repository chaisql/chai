-- test: basic
CREATE TABLE test(pk INT PRIMARY KEY, a INTEGER NOT NULL);
SELECT name, sql FROM __chai_catalog WHERE type = 'table' AND name = 'test';
/* result:
{
  "name": 'test',
  "sql": 'CREATE TABLE test (pk INTEGER NOT NULL, a INTEGER NOT NULL, CONSTRAINT test_pk PRIMARY KEY (pk))'
}
*/

-- test: not null twice
CREATE TABLE test(pk INT PRIMARY KEY, a INT NOT NULL NOT NULL);
-- error:
