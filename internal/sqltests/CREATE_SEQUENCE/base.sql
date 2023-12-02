-- test: no config
CREATE SEQUENCE seq;
SELECT * FROM __chai_catalog WHERE type = "sequence" AND name = "seq";
/* result:
{
  "name": "seq",
  "type": "sequence",
  "sql": "CREATE SEQUENCE seq"
}
*/

-- test: __chai_sequence table
CREATE SEQUENCE seq;
SELECT * FROM __chai_sequence WHERE name = "seq";
/* result:
{
  "name": "seq"
}
*/

-- test: IF NOT EXISTS
CREATE SEQUENCE IF NOT EXISTS seq;
SELECT * FROM __chai_catalog WHERE type = "sequence" AND name = "seq";
/* result:
{
  "name": "seq",
  "type": "sequence",
  "sql": "CREATE SEQUENCE seq"
}
*/

-- test: AS TINYINT
CREATE SEQUENCE seq AS TINYINT;
SELECT * FROM __chai_catalog WHERE type = "sequence" AND name = "seq";
/* result:
{
  "name": "seq",
  "type": "sequence",
  "sql": "CREATE SEQUENCE seq"
}
*/

-- test: AS DOUBLE
CREATE SEQUENCE seq AS DOUBLE;
-- error:

-- test: INCREMENT 10
CREATE SEQUENCE seq INCREMENT 10;
SELECT * FROM __chai_catalog WHERE type = "sequence" AND name = "seq";
/* result:
{
  "name": "seq",
  "type": "sequence",
  "sql": "CREATE SEQUENCE seq INCREMENT BY 10"
}
*/

-- test: INCREMENT BY 10
CREATE SEQUENCE seq INCREMENT BY 10;
SELECT * FROM __chai_catalog WHERE type = "sequence" AND name = "seq";
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
SELECT * FROM __chai_catalog WHERE type = "sequence" AND name = "seq";
/* result:
{
  "name": "seq",
  "type": "sequence",
  "sql": "CREATE SEQUENCE seq INCREMENT BY -10"
}
*/


-- test: NO MINVALUE
CREATE SEQUENCE seq NO MINVALUE;
SELECT * FROM __chai_catalog WHERE type = "sequence" AND name = "seq";
/* result:
{
  "name": "seq",
  "type": "sequence",
  "sql": "CREATE SEQUENCE seq"
}
*/

-- test: NO MAXVALUE
CREATE SEQUENCE seq NO MAXVALUE;
SELECT * FROM __chai_catalog WHERE type = "sequence" AND name = "seq";
/* result:
{
  "name": "seq",
  "type": "sequence",
  "sql": "CREATE SEQUENCE seq"
}
*/

-- test: NO CYCLE
CREATE SEQUENCE seq NO CYCLE;
SELECT * FROM __chai_catalog WHERE type = "sequence" AND name = "seq";
/* result:
{
  "name": "seq",
  "type": "sequence",
  "sql": "CREATE SEQUENCE seq"
}
*/

-- test: NO SUGAR
CREATE SEQUENCE seq NO SUGAR;
-- error:

-- test: MINVALUE 10
CREATE SEQUENCE seq MINVALUE 10;
SELECT * FROM __chai_catalog WHERE type = "sequence" AND name = "seq";
/* result:
{
  "name": "seq",
  "type": "sequence",
  "sql": "CREATE SEQUENCE seq MINVALUE 10"
}
*/

-- test: MINVALUE 'hello'
CREATE SEQUENCE seq MINVALUE 'hello';
-- error:

-- test: MAXVALUE 10
CREATE SEQUENCE seq MAXVALUE 10;
SELECT * FROM __chai_catalog WHERE type = "sequence" AND name = "seq";
/* result:
{
  "name": "seq",
  "type": "sequence",
  "sql": "CREATE SEQUENCE seq MAXVALUE 10"
}
*/

-- test: MAXVALUE 'hello'
CREATE SEQUENCE seq MAXVALUE 'hello';
-- error:

-- test: START WITH 10
CREATE SEQUENCE seq START WITH 10;
SELECT * FROM __chai_catalog WHERE type = "sequence" AND name = "seq";
/* result:
{
  "name": "seq",
  "type": "sequence",
  "sql": "CREATE SEQUENCE seq START WITH 10"
}
*/

-- test: START WITH 'hello'
CREATE SEQUENCE seq START WITH 'hello';
-- error:

-- test: START 10
CREATE SEQUENCE seq START 10;
SELECT * FROM __chai_catalog WHERE type = "sequence" AND name = "seq";
/* result:
{
  "name": "seq",
  "type": "sequence",
  "sql": "CREATE SEQUENCE seq START WITH 10"
}
*/

-- test: CACHE 10
CREATE SEQUENCE seq CACHE 10;
SELECT * FROM __chai_catalog WHERE type = "sequence" AND name = "seq";
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

-- test: CACHE 10
CREATE SEQUENCE seq CYCLE;
SELECT * FROM __chai_catalog WHERE type = "sequence" AND name = "seq";
/* result:
{
  "name": "seq",
  "type": "sequence",
  "sql": "CREATE SEQUENCE seq CYCLE"
}
*/
