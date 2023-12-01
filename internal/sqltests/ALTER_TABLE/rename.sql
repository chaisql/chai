-- setup:
CREATE TABLE test(a int primary key);

-- test: rename
ALTER TABLE test RENAME TO test2;
SELECT name, sql FROM __genji_catalog WHERE type = "table" AND (name = "test2" OR name = "test");
/* result:
{
  "name": "test2",
  "sql": "CREATE TABLE test2 (a INTEGER NOT NULL, CONSTRAINT test_pk PRIMARY KEY (a))"
}
*/

-- test: non-existing
ALTER TABLE unknown RENAME TO test2;
-- error:

-- test: duplicate
CREATE TABLE test2;
ALTER TABLE test2 RENAME TO test;
-- error:

-- test: reserved name
ALTER TABLE test RENAME TO __genji_catalog;
-- error:

-- test: bad syntax: no new name
ALTER TABLE test RENAME TO;
-- error:

-- test: bad syntax: no table name
ALTER TABLE RENAME TO test2;
-- error:

-- test: bad syntax: no TABLE
ALTER RENAME TABLE test TO test2;
-- error:

-- test: bad syntax: two identifiers for new name
ALTER TABLE test RENAME TO test2 test3;
-- error:

-- test: bad syntax: two identifiers for table name
ALTER TABLE test test2 RENAME TO test3;
-- error:



