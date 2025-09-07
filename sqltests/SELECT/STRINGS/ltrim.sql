-- setup:
CREATE TABLE test(
    pk INT PRIMARY KEY,
    a TEXT 
);

INSERT INTO test (pk, a) VALUES (1, " hello "), (2, "!hello!"),  (3, "     !hello!  ");

-- test: LTRIM TEXT default
SELECT LTRIM(a) FROM test;
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
SELECT LTRIM(a, "!") FROM test;
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
SELECT LTRIM(a, " !") FROM test;
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
SELECT LTRIM(a, "hel !") FROM test;
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
SELECT LTRIM(true);
/* result:
{
    "LTRIM(true)": NULL
}
*/

-- test: LTRIM INT
SELECT LTRIM(42);
/* result:
{
    "LTRIM(42)": NULL
}
*/

-- test: LTRIM DOUBLE
SELECT LTRIM(42.42);
/* result:
{
    "LTRIM(42.42)": NULL
}
*/

-- test: LTRIM STRING wrong param
SELECT LTRIM(" hello ", 42);
/* result:
{
    "LTRIM(\" hello \", 42)": NULL
}
*/
