-- setup:
CREATE TABLE test(
    pk INT PRIMARY KEY,
    a TEXT
);

INSERT INTO test (pk, a) VALUES (1, " hello "), (2, "!hello!"),  (3, "     !hello!  ");

-- test: TRIM TEXT default
SELECT TRIM(a) FROM test;
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
SELECT TRIM(a, "!") FROM test;
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
SELECT TRIM(a, " !") FROM test;
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
SELECT TRIM(a, "hel !") FROM test;
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
SELECT TRIM(true);
/* result:
{
    "TRIM(true)": NULL
}
*/

-- test: TRIM INT
SELECT TRIM(42);
/* result:
{
    "TRIM(42)": NULL
}
*/

-- test: TRIM DOUBLE
SELECT TRIM(42.42);
/* result:
{
    "TRIM(42.42)": NULL
}
*/

-- test: TRIM STRING wrong param
SELECT TRIM(" hello ", 42);
/* result:
{
    "TRIM(\" hello \", 42)": NULL
}
*/
