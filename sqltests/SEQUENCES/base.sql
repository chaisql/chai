-- test: no sequence
SELECT nextval('hello');
-- error:

-- test: unknown sequence
CREATE SEQUENCE seq1;
SELECT nextval('unknown');
-- error:

-- test: valid sequence
CREATE SEQUENCE seq1;
SELECT nextval('seq1');
/* result:
{
  "nextval(\"seq1\")": 1
}
*/

-- test: two times
CREATE SEQUENCE seq1;
SELECT nextval('seq1') as a, nextval('seq1') as b;
/* result:
{
  a: 1,
  b: 2
}