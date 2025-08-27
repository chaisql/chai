-- test: INCREMENT 10
CREATE SEQUENCE seq INCREMENT 10;
SELECT name, type, sql FROM __chai_catalog WHERE type = "sequence" AND name = "seq";
/* result:
{
  "name": "seq",
  "type": "sequence",
  "sql": "CREATE SEQUENCE seq INCREMENT BY 10"
}
*/

-- test: INCREMENT BY 10
CREATE SEQUENCE seq INCREMENT BY 10;
SELECT name, type, sql FROM __chai_catalog WHERE type = "sequence" AND name = "seq";
/* result:
{
  "name": "seq",
  "type": "sequence",
  "sql": "CREATE SEQUENCE seq INCREMENT BY 10"
}
*/

-- test: INCREMENT BY 0
CREATE SEQUENCE seq INCREMENT BY 0;
-- error:

-- test: INCREMENT BY -10
CREATE SEQUENCE seq INCREMENT BY -10;
SELECT name, type, sql FROM __chai_catalog WHERE type = "sequence" AND name = "seq";
/* result:
{
  "name": "seq",
  "type": "sequence",
  "sql": "CREATE SEQUENCE seq INCREMENT BY -10"
}
*/

-- test: INCREMENT shorthand negative
CREATE SEQUENCE seq INCREMENT -1;
SELECT name, type, sql FROM __chai_catalog WHERE type = "sequence" AND name = "seq";
/* result:
{
  "name": "seq",
  "type": "sequence",
  "sql": "CREATE SEQUENCE seq INCREMENT BY -1"
}
*/

-- test: INCREMENT shorthand zero
CREATE SEQUENCE seq INCREMENT 0;
-- error:
