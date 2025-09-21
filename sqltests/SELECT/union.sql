-- setup:
CREATE TABLE foo(a DOUBLE PRECISION PRIMARY KEY, b DOUBLE PRECISION);
CREATE TABLE bar(a DOUBLE PRECISION PRIMARY KEY, b DOUBLE PRECISION);
CREATE TABLE baz(x TEXT PRIMARY KEY, y TEXT);
INSERT INTO foo (a,b) VALUES (1.0, 1.0), (2.0, 2.0);
INSERT INTO bar (a,b) VALUES (2.0, 2.0), (3.0, 3.0);
INSERT INTO baz (x,y) VALUES ('a', 'a'), ('b', 'b');

-- test: basic union all
SELECT * FROM foo
UNION ALL
SELECT * FROM bar;
/* result:
{"a": 1.0, "b": 1.0}
{"a": 2.0, "b": 2.0}
{"a": 2.0, "b": 2.0}
{"a": 3.0, "b": 3.0}
*/

-- test: basic union all with diff fields
SELECT * FROM foo
UNION ALL
SELECT * FROM baz;
/* result:
{"a": 1.0, "b": 1.0}
{"a": 2.0, "b": 2.0}
{"a": 'a', "b": 'a'}
{"a": 'b', "b": 'b'}
*/

-- test: union all with conditions
SELECT * FROM foo WHERE a > 1
UNION ALL
SELECT * FROM baz WHERE x != 'b';
/* result:
{"a": 2.0, "b": 2.0}
{"a": 'a', "b": 'a'}
*/

-- test: self union all
SELECT * FROM foo WHERE a > 1
UNION ALL
SELECT * FROM foo WHERE a <= 1;
/* result:
{"a": 2.0, "b": 2.0}
{"a": 1.0, "b": 1.0}
*/

-- test: multiple unions all
SELECT * FROM foo
UNION ALL
SELECT * FROM bar
UNION ALL
SELECT * FROM baz;
/* result:
{"a": 1.0, "b": 1.0}
{"a": 2.0, "b": 2.0}
{"a": 2.0, "b": 2.0}
{"a": 3.0, "b": 3.0}
{"a": 'a', "b": 'a'}
{"a": 'b', "b": 'b'}
*/

-- test: basic union
SELECT * FROM foo
UNION
SELECT * FROM bar;
/* result:
{"a": 1.0, "b": 1.0}
{"a": 2.0, "b": 2.0}
{"a": 3.0, "b": 3.0}
*/

-- test: basic union all with diff fields
SELECT * FROM foo
UNION
SELECT * FROM baz;
/* result:
{"a": 1.0, "b": 1.0}
{"a": 2.0, "b": 2.0}
{"a": 'a', "b": 'a'}
{"a": 'b', "b": 'b'}
*/

-- test: union with conditions
SELECT * FROM foo WHERE a > 1
UNION
SELECT * FROM baz WHERE x != 'b';
/* result:
{"a": 2.0, "b": 2.0}
{"a": 'a', "b": 'a'}
*/

-- test: self union
SELECT * FROM foo
UNION
SELECT * FROM foo;
/* result:
{"a": 1.0, "b": 1.0}
{"a": 2.0, "b": 2.0}
*/

-- test: self union with conds
SELECT * FROM foo WHERE a > 1
UNION
SELECT * FROM foo WHERE a <= 1;
/* result:
{"a": 1.0, "b": 1.0}
{"a": 2.0, "b": 2.0}
*/

-- test: multiple unions all
SELECT * FROM foo
UNION ALL
SELECT * FROM bar
UNION ALL
SELECT * FROM baz;
/* result:
{"a": 1.0, "b": 1.0}
{"a": 2.0, "b": 2.0}
{"a": 2.0, "b": 2.0}
{"a": 3.0, "b": 3.0}
{"a": 'a', "b": 'a'}
{"a": 'b', "b": 'b'}
*/

-- test: combined unions
SELECT * FROM foo
UNION
SELECT * FROM bar
UNION ALL
SELECT * FROM baz;
/* result:
{"a": 1.0, "b": 1.0}
{"a": 2.0, "b": 2.0}
{"a": 3.0, "b": 3.0}
{"a": 'a', "b": 'a'}
{"a": 'b', "b": 'b'}