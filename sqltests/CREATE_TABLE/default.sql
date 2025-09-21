-- test: same type
CREATE TABLE test(pk INT PRIMARY KEY, a INT DEFAULT 10);
SELECT name, sql FROM __chai_catalog WHERE type = 'table' AND name = 'test';
/* result:
{
  "name": 'test',
  "sql": 'CREATE TABLE test (pk INTEGER NOT NULL, a INTEGER DEFAULT 10, CONSTRAINT test_pk PRIMARY KEY (pk))'
}
*/

-- test: compatible type
CREATE TABLE test(pk INT PRIMARY KEY, a DOUBLE PRECISION DEFAULT 10);
SELECT name, sql FROM __chai_catalog WHERE type = 'table' AND name = 'test';
/* result:
{
  "name": 'test',
  "sql": 'CREATE TABLE test (pk INTEGER NOT NULL, a DOUBLE PRECISION DEFAULT 10, CONSTRAINT test_pk PRIMARY KEY (pk))'
}
*/

-- test: expr
CREATE TABLE test(pk INT PRIMARY KEY, a DOUBLE PRECISION DEFAULT 1 + 4 / 4);
SELECT name, sql FROM __chai_catalog WHERE type = 'table' AND name = 'test';
/* result:
{
  "name": 'test',
  "sql": 'CREATE TABLE test (pk INTEGER NOT NULL, a DOUBLE PRECISION DEFAULT 1 + 4 / 4, CONSTRAINT test_pk PRIMARY KEY (pk))'
}
*/

-- test: incompatible type
CREATE TABLE test(pk INT PRIMARY KEY, a DOUBLE PRECISION DEFAULT 'hello');
-- error:

-- test: function
CREATE TABLE test(pk INT PRIMARY KEY, a DOUBLE PRECISION DEFAULT pk());
-- error:

-- test: compatible type
CREATE TABLE test(pk INT PRIMARY KEY, a DOUBLE PRECISION DEFAULT 10);
SELECT name, sql FROM __chai_catalog WHERE type = 'table' AND name = 'test';
/* result:
{
  "name": 'test',
  "sql": 'CREATE TABLE test (pk INTEGER NOT NULL, a DOUBLE PRECISION DEFAULT 10, CONSTRAINT test_pk PRIMARY KEY (pk))'
}
*/

-- test: expr
CREATE TABLE test(pk INT PRIMARY KEY, a DOUBLE PRECISION DEFAULT 1 + 4 / 4);
SELECT name, sql FROM __chai_catalog WHERE type = 'table' AND name = 'test';
/* result:
{
  "name": 'test',
  "sql": 'CREATE TABLE test (pk INTEGER NOT NULL, a DOUBLE PRECISION DEFAULT 1 + 4 / 4, CONSTRAINT test_pk PRIMARY KEY (pk))'
}
*/

-- test: incompatible type
CREATE TABLE test(pk INT PRIMARY KEY, a DOUBLE PRECISION DEFAULT 'hello');
-- error:

-- test: function
CREATE TABLE test(pk INT PRIMARY KEY, a DOUBLE PRECISION DEFAULT pk());
-- error:
-- test: same type
CREATE TABLE test(pk INT PRIMARY KEY, a INT DEFAULT 10);
SELECT name, sql FROM __chai_catalog WHERE type = 'table' AND name = 'test';
/* result:
{
  "name": 'test',
  "sql": 'CREATE TABLE test (pk INTEGER NOT NULL, a INTEGER DEFAULT 10, CONSTRAINT test_pk PRIMARY KEY (pk))'
}
*/

-- test: compatible type
CREATE TABLE test(pk INT PRIMARY KEY, a DOUBLE PRECISION DEFAULT 10);
SELECT name, sql FROM __chai_catalog WHERE type = 'table' AND name = 'test';
/* result:
{
  "name": 'test',
  "sql": 'CREATE TABLE test (pk INTEGER NOT NULL, a DOUBLE PRECISION DEFAULT 10, CONSTRAINT test_pk PRIMARY KEY (pk))'
}
*/

-- test: expr
CREATE TABLE test(pk INT PRIMARY KEY, a DOUBLE PRECISION DEFAULT 1 + 4 / 4);
SELECT name, sql FROM __chai_catalog WHERE type = 'table' AND name = 'test';
/* result:
{
  "name": 'test',
  "sql": 'CREATE TABLE test (pk INTEGER NOT NULL, a DOUBLE PRECISION DEFAULT 1 + 4 / 4, CONSTRAINT test_pk PRIMARY KEY (pk))'
}
*/

-- test: incompatible type
CREATE TABLE test(pk INT PRIMARY KEY, a DOUBLE PRECISION DEFAULT 'hello');
-- error:

-- test: function
CREATE TABLE test(pk INT PRIMARY KEY, a DOUBLE PRECISION DEFAULT pk());
-- error:

-- test: incompatible expr
CREATE TABLE test(pk INT PRIMARY KEY, a BYTEA DEFAULT 1 + 4 / 4);
-- error:

-- test: forbidden tokens: AND
CREATE TABLE test(pk INT PRIMARY KEY, a BYTEA DEFAULT 1 AND 1);
-- error:

-- test: forbidden tokens: path
CREATE TABLE test(pk INT PRIMARY KEY, a BYTEA DEFAULT b);
-- error:

-- test: DEFAULT nextval sequence
CREATE SEQUENCE seq1;
CREATE TABLE test(pk INT PRIMARY KEY, a INTEGER DEFAULT nextval('seq1'));
SELECT name, sql FROM __chai_catalog WHERE type = 'table' AND name = 'test';
/* result:
{
  "name": 'test',
  "sql": 'CREATE TABLE test (pk INTEGER NOT NULL, a INTEGER DEFAULT nextval(\'seq1\'), CONSTRAINT test_pk PRIMARY KEY (pk))'
}
*/

-- test: DEFAULT with parentheses
CREATE TABLE test(pk INT PRIMARY KEY, a INTEGER DEFAULT (1 + 2));
SELECT name, sql FROM __chai_catalog WHERE type = 'table' AND name = 'test';
/* result:
{
  "name": 'test',
  "sql": 'CREATE TABLE test (pk INTEGER NOT NULL, a INTEGER DEFAULT 1 + 2, CONSTRAINT test_pk PRIMARY KEY (pk))'
}
*/

-- test: DEFAULT (nextval sequence)
CREATE SEQUENCE seq_paren;
CREATE TABLE test(pk INT PRIMARY KEY, a INTEGER DEFAULT (nextval('seq_paren')));
SELECT name, sql FROM __chai_catalog WHERE type = 'table' AND name = 'test';
/* result:
{
  "name": 'test',
  "sql": 'CREATE TABLE test (pk INTEGER NOT NULL, a INTEGER DEFAULT nextval(\'seq_paren\'), CONSTRAINT test_pk PRIMARY KEY (pk))'
}
*/

-- test: DEFAULT with nested parentheses
CREATE TABLE test(pk INT PRIMARY KEY, a INTEGER DEFAULT ((1)));
SELECT name, sql FROM __chai_catalog WHERE type = 'table' AND name = 'test';
/* result:
{
  "name": 'test',
  "sql": 'CREATE TABLE test (pk INTEGER NOT NULL, a INTEGER DEFAULT (1), CONSTRAINT test_pk PRIMARY KEY (pk))'
}
*/

