-- setup:
CREATE TABLE test(a timestamp);
INSERT INTO test (a) VALUES ("2023"), ("2025"), ("2021"), ("2000");

-- suite: no index

-- suite: with index
CREATE INDEX ON test(a);

-- test: asc
SELECT a FROM test ORDER BY a;
/* result:
{
    a: "2000-01-01T00:00:00Z",
}
{
    a: "2021-01-01T00:00:00Z"
}
{
    a: "2023-01-01T00:00:00Z"
}
{
    a: "2025-01-01T00:00:00Z"
}
*/

-- test: asc / wildcard
SELECT * FROM test ORDER BY a;
/* result:
{
    a: "2000-01-01T00:00:00Z",
}
{
    a: "2021-01-01T00:00:00Z"
}
{
    a: "2023-01-01T00:00:00Z"
}
{
    a: "2025-01-01T00:00:00Z"
}
*/

-- test: desc
SELECT a FROM test ORDER BY a DESC;
/* result:
{
    a: "2025-01-01T00:00:00Z",
}
{
    a: "2023-01-01T00:00:00Z"
}
{
    a: "2021-01-01T00:00:00Z"
}
{
    a: "2000-01-01T00:00:00Z"
}
*/

-- test: desc / wildcard
SELECT * FROM test ORDER BY a DESC;
/* result:
{
    a: "2025-01-01T00:00:00Z",
}
{
    a: "2023-01-01T00:00:00Z"
}
{
    a: "2021-01-01T00:00:00Z"
}
{
    a: "2000-01-01T00:00:00Z"
}
*/
