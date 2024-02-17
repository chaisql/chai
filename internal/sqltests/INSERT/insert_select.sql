-- setup:
CREATE TABLE foo(a INT, b INT, c INT, d INT, e INT);
CREATE TABLE bar(a INT, b INT);
INSERT INTO bar (a, b) VALUES (1, 10);

-- test: same table
INSERT INTO foo SELECT * FROM foo;
-- error:

-- test: No columns / No projection
INSERT INTO foo SELECT * FROM bar;
SELECT * FROM foo;
/* result:
{
    "a":1,
    "b":10,
    "c":null,
    "d":null,
    "e":null
}
*/

-- test: No columns / Projection
INSERT INTO foo SELECT a FROM bar;
SELECT * FROM foo;
/* result:
{
    "a":1,
    "b":null,
    "c":null,
    "d":null,
    "e":null
}
*/

-- test: With columns / No Projection
INSERT INTO foo (a, b) SELECT * FROM bar;
SELECT * FROM foo;
/* result:
{
    "a":1,
    "b":10,
    "c":null,
    "d":null,
    "e":null
}
*/

-- test: With columns / Projection
INSERT INTO foo (c, d) SELECT a, b FROM bar;
SELECT * FROM foo;
/* result:
{
    "a":null,
    "b":null,
    "c":1,
    "d":10,
    "e":null
}
*/

-- test: Too many columns / No Projection
INSERT INTO foo (c) SELECT * FROM bar;
-- error:

-- test: Too many columns / Projection
INSERT INTO foo (c, d) SELECT a, b, c FROM bar;
-- error:

-- test: Too few columns / No Projection
INSERT INTO foo (c, d, e) SELECT * FROM bar;
-- error:

-- test: Too few columns / Projection
INSERT INTO foo (c, d) SELECT a FROM bar;
-- error:
