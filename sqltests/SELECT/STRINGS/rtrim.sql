-- setup:
CREATE TABLE test(
    a TEXT
);

INSERT INTO test (a) VALUES (" hello "), ("!hello!"),  ("     !hello!  ");

-- test: RTRIM TEXT default
SELECT RTRIM(a) FROM test;
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
SELECT RTRIM(a, "!") FROM test;
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
SELECT RTRIM(a, " !") FROM test;
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
SELECT RTRIM(a, "hel !") FROM test;
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
SELECT RTRIM(true);
/* result:
{
    "RTRIM(true)": NULL
}
*/

-- test: RTRIM INT
SELECT RTRIM(42);
/* result:
{
    "RTRIM(42)": NULL
}
*/

-- test: RTRIM DOUBLE
SELECT RTRIM(42.42);
/* result:
{
    "RTRIM(42.42)": NULL
}
*/

-- test: RTRIM STRING wrong param
SELECT RTRIM(" hello ", 42);
/* result:
{
    "RTRIM(\" hello \", 42)": NULL
}
*/
