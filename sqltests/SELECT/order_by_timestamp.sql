-- setup:
CREATE TABLE test(pk int primary key, a timestamp );
INSERT INTO test VALUES (1, "2023"), (2, "2025"), (3, "2021"), (4, "2000");

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
    pk: 4,
    a: "2000-01-01T00:00:00Z",
}
{
    pk: 3,
    a: "2021-01-01T00:00:00Z"
}
{
    pk: 1,
    a: "2023-01-01T00:00:00Z"
}
{
    pk: 2,
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
    pk: 2,
    a: "2025-01-01T00:00:00Z",
}
{
    pk: 1,
    a: "2023-01-01T00:00:00Z"
}
{
    pk: 3,
    a: "2021-01-01T00:00:00Z"
}
{
    pk: 4,
    a: "2000-01-01T00:00:00Z"
}
*/
