-- setup:
CREATE TABLE foo(
  a TEXT,
  b ARRAY,
  c (
      ...
  )
);
INSERT INTO foo VALUES (
  "hello",
  [1, 2, 3, [4, 5]],
  {
    a: 1,
    b: 2,
    c: {
      d: 3
    }
  }
);

-- test: text field
SELECT len(a) FROM foo;
/* result:
{
    "LEN(a)": 5
}
*/

-- test: array field
SELECT len(b) FROM foo;
/* result:
{
    "LEN(b)": 4
}
*/

-- test: document
SELECT len(c) FROM foo;
/* result:
{
    "LEN(c)": 3
}
*/

-- test: subarray
SELECT len(b[3]) FROM foo;
/* result:
{
    "LEN(b[3])": 2
}
*/

-- test: subdocument
SELECT len(c.c) FROM foo;
/* result:
{
    "LEN(c.c)": 1
}
*/

-- test: text expr
SELECT len("hello, world!");
/* result:
{
  "LEN(\"hello, world!\")": 13
}
*/

-- test: zero text expr
SELECT len('');
/* result:
{
  "LEN(\"\")": 0
}
*/

-- test: array expr
SELECT len([1, 2, 3, 4, 5]);
/* result:
{
  "LEN([1, 2, 3, 4, 5])": 5
}
*/

-- test: empty array expr
SELECT len([]);
/* result:
{
  "LEN([])": 0
}
*/

-- test: mixed type array expr
SELECT len([1, 2, 3, [1, 2, 3]]);
/* result:
{
  "LEN([1, 2, 3, [1, 2, 3]])": 4
}
*/

-- test: document expr
SELECT len({'a': 1, 'b': 2, 'c': 3});
/* result:
{
  "LEN({a: 1, b: 2, c: 3})": 3
}
*/

-- test: empty document expr
SELECT len({});
/* result:
{
  "LEN({})": 0
}
*/

-- test: integer expr
SELECT len(10);
/* result:
{
  "LEN(10)": NULL
}
*/

-- test: float expr
SELECT len(1.0);
/* result:
{
  "LEN(1.0)": NULL
}
*/

-- test: NULL expr
SELECT len(NULL);
/* result:
{
  "LEN(NULL)": NULL
}
*/

-- test: NULL expr
SELECT len(NULL);
/* result:
{
  "LEN(NULL)": NULL
}
*/

-- test: blob expr
SELECT len('\x323232') as l;
/* result:
{
  "l": NULL
}
*/
