-- test: CACHE 10
CREATE SEQUENCE seq CACHE 10;
SELECT name, type, sql FROM __chai_catalog WHERE type = "sequence" AND name = "seq";
/* result:
{
  "name": "seq",
  "type": "sequence",
  "sql": "CREATE SEQUENCE seq CACHE 10"
}
*/

-- test: CACHE 'hello'
CREATE SEQUENCE seq CACHE 'hello';
-- error:

-- test: CACHE -10
CREATE SEQUENCE seq CACHE -10;
-- error:

-- test: CACHE 0
CREATE SEQUENCE seq CACHE 0;
-- error:

-- test: CACHE 1 should be default and not printed
CREATE SEQUENCE seq CACHE 1;
SELECT name, type, sql FROM __chai_catalog WHERE type = "sequence" AND name = "seq";
/* result:
{
  "name": "seq",
  "type": "sequence",
  "sql": "CREATE SEQUENCE seq"
}
*/

-- test: CYCLE
CREATE SEQUENCE seq CYCLE;
SELECT name, type, sql FROM __chai_catalog WHERE type = "sequence" AND name = "seq";
/* result:
{
  "name": "seq",
  "type": "sequence",
  "sql": "CREATE SEQUENCE seq CYCLE"
}
*/
