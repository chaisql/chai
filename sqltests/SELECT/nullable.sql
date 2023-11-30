-- test: document with no constraint
CREATE TABLE test (a OBJECT, c int);
INSERT INTO test (a) VALUES ({ b: 1 }), ({ b: 2 });
SELECT * FROM test;
/* result:
{
    a: { b: 1.0 },
}
{
    a: { b: 2.0 },
}
*/

-- test: one nullable column
CREATE TABLE test (a INT);
INSERT INTO test (a) VALUES (null), (null);
SELECT * FROM test;
/* result:
{}
{}
*/

-- test: first column null
CREATE TABLE test (a INT, b INT);
INSERT INTO test (b) VALUES (1), (2);
SELECT * FROM test;
/* result:
{
    b: 1
}
{
    b: 2
}
*/

-- test: second column null
CREATE TABLE test (a INT, b INT);
INSERT INTO test (a) VALUES (1), (2);
SELECT * FROM test;
/* result:
{
    a: 1
}
{
    a: 2
}
*/

-- test: after a document 
CREATE TABLE test (a OBJECT, b INT);
INSERT INTO test (a) VALUES ({ c: 1 }), ({ c: 2 });
SELECT * FROM test;
/* result:
{
    a: { c: 1.0 }
}
{
    a: { c: 2.0 }
}
*/