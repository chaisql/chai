-- test: ORDER 1 (combined options)
CREATE SEQUENCE seq AS INTEGER INCREMENT BY 2 NO MINVALUE MAXVALUE 10 START WITH 5 CACHE 5 CYCLE;
SELECT name, type, sql FROM __chai_catalog WHERE type = "sequence" AND name = "seq";
/* result:
{
  "name": "seq",
  "type": "sequence",
  "sql": "CREATE SEQUENCE seq INCREMENT BY 2 MAXVALUE 10 START WITH 5 CACHE 5 CYCLE"
}
*/

-- test: ORDER 2 (same options different order)
CREATE SEQUENCE seq CYCLE MAXVALUE 10 INCREMENT BY 2 START WITH 5 AS INTEGER NO MINVALUE CACHE 5;
SELECT name, type, sql FROM __chai_catalog WHERE type = "sequence" AND name = "seq";
/* result:
{
  "name": "seq",
  "type": "sequence",
  "sql": "CREATE SEQUENCE seq INCREMENT BY 2 MAXVALUE 10 START WITH 5 CACHE 5 CYCLE"
}
*/

-- test: duplicate AS INT
CREATE SEQUENCE seq AS INT AS INT;
-- error:

-- test: duplicate INCREMENT BY
CREATE SEQUENCE seq INCREMENT BY 10 INCREMENT BY 10;
-- error:

-- test: duplicate NO MINVALUE
CREATE SEQUENCE seq NO MINVALUE NO MINVALUE;
-- error:

-- test: MINVALUE > MAXVALUE (bad range)
CREATE SEQUENCE seq MINVALUE 10 MAXVALUE 5;
-- error:

-- test: START greater than MAX
CREATE SEQUENCE seq MINVALUE 5 MAXVALUE 10 START 100;
-- error:

-- test: START lower than MIN
CREATE SEQUENCE seq MINVALUE 5 MAXVALUE 10 START -100;
-- error:

-- test: DESC MINVALUE/MAXVALUE
CREATE SEQUENCE seq MINVALUE 10 MAXVALUE 100 INCREMENT BY -1;
SELECT name, type, sql FROM __chai_catalog WHERE type = "sequence" AND name = "seq";
/* result:
{
  "name": "seq",
  "type": "sequence",
  "sql": "CREATE SEQUENCE seq INCREMENT BY -1 MINVALUE 10 MAXVALUE 100"
}
*/

-- test: NO MINVALUE DESC
CREATE SEQUENCE seq NO MINVALUE MAXVALUE 100 INCREMENT BY -1;
SELECT name, type, sql FROM __chai_catalog WHERE type = "sequence" AND name = "seq";
/* result:
{
  "name": "seq",
  "type": "sequence",
  "sql": "CREATE SEQUENCE seq INCREMENT BY -1 MAXVALUE 100"
}
*/

-- test: NO MAXVALUE DESC (should only print increment)
CREATE SEQUENCE seq NO MINVALUE NO MAXVALUE INCREMENT BY -1;
SELECT name, type, sql FROM __chai_catalog WHERE type = "sequence" AND name = "seq";
/* result:
{
  "name": "seq",
  "type": "sequence",
  "sql": "CREATE SEQUENCE seq INCREMENT BY -1"
}
*/
