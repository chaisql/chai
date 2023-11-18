-- setup:
CREATE TABLE test(
    a TEXT
);

INSERT INTO test (a) VALUES (" hello "), ("!hello!"),  ("     !hello!  ");

-- test: RTRIM TEXT default
SELECT strings.RTRIM(a) FROM test;
/* result:
{
    "RTRIM(a)": " hello"
}
{
    "RTRIM(a)": "!hello!"
}
{
    "RTRIM(a)": "     !hello!"
}
*/


-- test: RTRIM TEXT with param
SELECT strings.RTRIM(a, "!") FROM test;
/* result:
{
    "RTRIM(a, \"!\")": " hello "
}
{
    "RTRIM(a, \"!\")": "!hello"
}
{
    "RTRIM(a, \"!\")": "     !hello!  "
}
*/

-- test: RTRIM TEXT with multiple char params
SELECT strings.RTRIM(a, " !") FROM test;
/* result:
{
    "RTRIM(a, \" !\")": " hello"
}
{
    "RTRIM(a, \" !\")": "!hello"
}
{
    "RTRIM(a, \" !\")": "     !hello"
}
*/


-- test: RTRIM TEXT with multiple char params
SELECT strings.RTRIM(a, "hel !") FROM test;
/* result:
{
    "RTRIM(a, \"hel !\")": " hello"
}
{
    "RTRIM(a, \"hel !\")": "!hello"
}
{
    "RTRIM(a, \"hel !\")": "     !hello"
}
*/


-- test: RTRIM BOOL
SELECT strings.RTRIM(true);
/* result:
{
    "RTRIM(true)": NULL
}
*/

-- test: RTRIM INT
SELECT strings.RTRIM(42);
/* result:
{
    "RTRIM(42)": NULL
}
*/

-- test: RTRIM DOUBLE
SELECT strings.RTRIM(42.42);
/* result:
{
    "RTRIM(42.42)": NULL
}
*/

-- test: RTRIM ARRAY
SELECT strings.RTRIM([1, 2]);
/* result:
{
    "RTRIM([1, 2])": NULL
}
*/
-- test: RTRIM DOCUMENT
SELECT strings.RTRIM({a: 1});
/* result:
{
    "RTRIM({a: 1})": NULL
}
*/

-- test: RTRIM STRING wrong param
SELECT strings.RTRIM(" hello ", 42);
/* result:
{
    "RTRIM(\" hello \", 42)": NULL
}
*/
