-- test: missing name
CREATE SEQUENCE;
-- error:

-- test: IF NOT EXISTS but missing name
CREATE SEQUENCE IF NOT EXISTS;
-- error:

-- test: duplicate INCREMENT (mixed forms)
CREATE SEQUENCE seq INCREMENT BY 2 INCREMENT 2;
-- error:

-- test: duplicate MINVALUE
CREATE SEQUENCE seq MINVALUE 5 MINVALUE 6;
-- error:

-- test: duplicate MAXVALUE
CREATE SEQUENCE seq MAXVALUE 10 MAXVALUE 11;
-- error:

-- test: duplicate START
CREATE SEQUENCE seq START 1 START 2;
-- error:

-- test: duplicate CACHE
CREATE SEQUENCE seq CACHE 2 CACHE 3;
-- error:

-- test: duplicate CYCLE
CREATE SEQUENCE seq CYCLE CYCLE;
-- error:

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

-- test: CACHE 0 should be invalid in Postgres: expect error
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

-- test: INCREMENT shorthand zero
CREATE SEQUENCE seq INCREMENT 0;
-- error:
