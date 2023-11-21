-- setup:
CREATE TABLE test(
    a TEXT
);

INSERT INTO test (a) VALUES (" hello "), ("!hello!"),  ("     !hello!  ");

-- test: LTRIM TEXT default
SELECT strings.LTRIM(a) FROM test;
/* result:
{
    "LTRIM(a)": "hello "
}
{
    "LTRIM(a)": "!hello!"
}
{
    "LTRIM(a)": "!hello!  "
}
*/


-- test: LTRIM TEXT with param
SELECT strings.LTRIM(a, "!") FROM test;
/* result:
{
    "LTRIM(a, \"!\")": " hello "
}
{
    "LTRIM(a, \"!\")": "hello!"
}
{
    "LTRIM(a, \"!\")": "     !hello!  "
}
*/

-- test: LTRIM TEXT with multiple char params
SELECT strings.LTRIM(a, " !") FROM test;
/* result:
{
    "LTRIM(a, \" !\")": "hello "
}
{
    "LTRIM(a, \" !\")": "hello!"
}
{
    "LTRIM(a, \" !\")": "hello!  "
}
*/


-- test: LTRIM TEXT with multiple char params
SELECT strings.LTRIM(a, "hel !") FROM test;
/* result:
{
    "LTRIM(a, \"hel !\")": "o "
}
{
    "LTRIM(a, \"hel !\")": "o!"
}
{
    "LTRIM(a, \"hel !\")": "o!  "
}
*/


-- test: LTRIM BOOL
SELECT strings.LTRIM(true);
/* result:
{
    "LTRIM(true)": NULL
}
*/

-- test: LTRIM INT
SELECT strings.LTRIM(42);
/* result:
{
    "LTRIM(42)": NULL
}
*/

-- test: LTRIM DOUBLE
SELECT strings.LTRIM(42.42);
/* result:
{
    "LTRIM(42.42)": NULL
}
*/

-- test: LTRIM ARRAY
SELECT strings.LTRIM([1, 2]);
/* result:
{
    "LTRIM([1, 2])": NULL
}
*/
-- test: LTRIM DOCUMENT
SELECT strings.LTRIM({a: 1});
/* result:
{
    "LTRIM({a: 1})": NULL
}
*/

-- test: LTRIM STRING wrong param
SELECT strings.LTRIM(" hello ", 42);
/* result:
{
    "LTRIM(\" hello \", 42)": NULL
}
*/
