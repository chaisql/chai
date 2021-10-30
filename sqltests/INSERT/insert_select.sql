-- setup:
CREATE TABLE foo;
CREATE TABLE bar;
INSERT INTO bar (a, b) VALUES (1, 10);

-- test: same table
INSERT INTO foo SELECT * FROM foo;
-- error:

-- test: No fields / No projection
INSERT INTO foo SELECT * FROM bar;
SELECT pk(), * FROM foo;
/* result:
{"pk()":1, "a":1.0, "b":10.0}
*/

-- test: No fields / Projection
INSERT INTO foo SELECT a FROM bar;
SELECT pk(), * FROM foo;
/* result:
{"pk()":1, "a":1.0}
*/

-- test: With fields / No Projection
INSERT INTO foo (a, b) SELECT * FROM bar;
SELECT pk(), * FROM foo;
/* result:
{"pk()":1, "a":1.0, "b":10.0}
*/

-- test: With fields / Projection
INSERT INTO foo (c, d) SELECT a, b FROM bar;
SELECT pk(), * FROM foo;
/* result:
{"pk()":1, "c":1.0, "d":10.0}
*/

-- test: Too many fields / No Projection
INSERT INTO foo (c) SELECT * FROM bar;
-- error:

-- test: Too many fields / Projection
INSERT INTO foo (c, d) SELECT a, b, c FROM bar;
-- error:

-- test: Too few fields / No Projection
INSERT INTO foo (c, d, e) SELECT * FROM bar;
-- error:

-- test: Too few fields / Projection
INSERT INTO foo (c, d) SELECT a FROM bar`;
-- error:
