-- test: AS TINYINT (use bigint for Postgres compatibility)
CREATE SEQUENCE seq AS bigint;
SELECT name, type, sql FROM __chai_catalog WHERE type = "sequence" AND name = "seq";
/* result:
{
  "name": "seq",
  "type": "sequence",
  "sql": "CREATE SEQUENCE seq"
}
*/

-- test: AS DOUBLE is invalid in Postgres: keep as error
CREATE SEQUENCE seq AS DOUBLE;
-- error:

-- test: AS INT is accepted (no-op in canonical SQL)
CREATE SEQUENCE seq AS INT;
SELECT name, type, sql FROM __chai_catalog WHERE type = "sequence" AND name = "seq";
/* result:
{
  "name": "seq",
  "type": "sequence",
  "sql": "CREATE SEQUENCE seq"
}
*/

-- test: AS SMALLINT is accepted (no-op in canonical SQL)
CREATE SEQUENCE seq AS SMALLINT;
SELECT name, type, sql FROM __chai_catalog WHERE type = "sequence" AND name = "seq";
/* result:
{
  "name": "seq",
  "type": "sequence",
  "sql": "CREATE SEQUENCE seq"
}
*/
