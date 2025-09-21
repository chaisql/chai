-- test: NO MINVALUE
CREATE SEQUENCE seq NO MINVALUE;
SELECT name, type, sql FROM __chai_catalog WHERE type = 'sequence' AND name = 'seq';
/* result:
{
  "name": 'seq',
  "type": 'sequence',
  "sql": 'CREATE SEQUENCE seq'
}
*/

-- test: NO MAXVALUE
CREATE SEQUENCE seq NO MAXVALUE;
SELECT name, type, sql FROM __chai_catalog WHERE type = 'sequence' AND name = 'seq';
/* result:
{
  "name": 'seq',
  "type": 'sequence',
  "sql": 'CREATE SEQUENCE seq'
}
*/

-- test: MINVALUE 10
CREATE SEQUENCE seq MINVALUE 10;
SELECT name, type, sql FROM __chai_catalog WHERE type = 'sequence' AND name = 'seq';
/* result:
{
  "name": 'seq',
  "type": 'sequence',
  "sql": 'CREATE SEQUENCE seq MINVALUE 10'
}
*/

-- test: MINVALUE 'hello'
CREATE SEQUENCE seq MINVALUE 'hello';
-- error:

-- test: MAXVALUE 10
CREATE SEQUENCE seq MAXVALUE 10;
SELECT name, type, sql FROM __chai_catalog WHERE type = 'sequence' AND name = 'seq';
/* result:
{
  "name": 'seq',
  "type": 'sequence',
  "sql": 'CREATE SEQUENCE seq MAXVALUE 10'
}
*/

-- test: MAXVALUE 'hello'
CREATE SEQUENCE seq MAXVALUE 'hello';
-- error:

-- test: START WITH 10
CREATE SEQUENCE seq START WITH 10;
SELECT name, type, sql FROM __chai_catalog WHERE type = 'sequence' AND name = 'seq';
/* result:
{
  "name": 'seq',
  "type": 'sequence',
  "sql": 'CREATE SEQUENCE seq START WITH 10'
}
*/

-- test: START WITH 'hello'
CREATE SEQUENCE seq START WITH 'hello';
-- error:

-- test: START 10
CREATE SEQUENCE seq START 10;
SELECT name, type, sql FROM __chai_catalog WHERE type = 'sequence' AND name = 'seq';
/* result:
{
  "name": 'seq',
  "type": 'sequence',
  "sql": 'CREATE SEQUENCE seq START WITH 10'
}
*/

-- test: START equal to default should be omitted
CREATE SEQUENCE seq START 1;
SELECT name, type, sql FROM __chai_catalog WHERE type = 'sequence' AND name = 'seq';
/* result:
{
  "name": 'seq',
  "type": 'sequence',
  "sql": 'CREATE SEQUENCE seq'
}
*/
