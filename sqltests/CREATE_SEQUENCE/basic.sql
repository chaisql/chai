-- test: no config
CREATE SEQUENCE seq;
SELECT name, type, sql FROM __chai_catalog WHERE type = 'sequence' AND name = 'seq';
/* result:
{
  "name": 'seq',
  "type": 'sequence',
  "sql": 'CREATE SEQUENCE seq'
}
*/

-- test: __chai_sequence table
CREATE SEQUENCE seq;
SELECT name FROM __chai_sequence WHERE name = 'seq';
/* result:
{
  "name": 'seq'
}
*/

-- test: IF NOT EXISTS
CREATE SEQUENCE IF NOT EXISTS seq;
SELECT name, type, sql FROM __chai_catalog WHERE type = 'sequence' AND name = 'seq';
/* result:
{
  "name": 'seq',
  "type": 'sequence',
  "sql": 'CREATE SEQUENCE seq'
}
*/

-- test: IF NOT EXISTS is idempotent
CREATE SEQUENCE IF NOT EXISTS seq;
CREATE SEQUENCE IF NOT EXISTS seq;
SELECT COUNT(*) AS c FROM __chai_sequence WHERE name = 'seq';
/* result:
{
  "c": 1
}
*/
