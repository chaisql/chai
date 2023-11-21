-- setup:
CREATE TABLE test(
    a TEXT
);

INSERT INTO test (a) VALUES (" hello "), ("!hello!"),  ("     !hello!  ");

-- test: TRIM TEXT default
SELECT strings.TRIM(a) FROM test;
/* result:
{
    "TRIM(a)": "hello"
}
{
    "TRIM(a)": "!hello!"
}
{
    "TRIM(a)": "!hello!"
}
*/


-- test: TRIM TEXT with param
SELECT strings.TRIM(a, "!") FROM test;
/* result:
{
    "TRIM(a, \"!\")": " hello "
}
{
    "TRIM(a, \"!\")": "hello"
}
{
    "TRIM(a, \"!\")": "     !hello!  "
}
*/

-- test: TRIM TEXT with multiple char params
SELECT strings.TRIM(a, " !") FROM test;
/* result:
{
    "TRIM(a, \" !\")": "hello"
}
{
    "TRIM(a, \" !\")": "hello"
}
{
    "TRIM(a, \" !\")": "hello"
}
*/


-- test: TRIM TEXT with multiple char params
SELECT strings.TRIM(a, "hel !") FROM test;
/* result:
{
    "TRIM(a, \"hel !\")": "o"
}
{
    "TRIM(a, \"hel !\")": "o"
}
{
    "TRIM(a, \"hel !\")": "o"
}
*/


-- test: TRIM BOOL
SELECT strings.TRIM(true);
/* result:
{
    "TRIM(true)": NULL
}
*/

-- test: TRIM INT
SELECT strings.TRIM(42);
/* result:
{
    "TRIM(42)": NULL
}
*/

-- test: TRIM DOUBLE
SELECT strings.TRIM(42.42);
/* result:
{
    "TRIM(42.42)": NULL
}
*/

-- test: TRIM ARRAY
SELECT strings.TRIM([1, 2]);
/* result:
{
    "TRIM([1, 2])": NULL
}
*/
-- test: TRIM DOCUMENT
SELECT strings.TRIM({a: 1});
/* result:
{
    "TRIM({a: 1})": NULL
}
*/

-- test: TRIM STRING wrong param
SELECT strings.TRIM(" hello ", 42);
/* result:
{
    "TRIM(\" hello \", 42)": NULL
}
*/
