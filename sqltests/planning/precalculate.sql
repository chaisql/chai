-- setup:
CREATE table test(pk int primary key, a int);

-- test: precalculate constant
EXPLAIN SELECT * FROM test WHERE 3 + 4 > a + 3 % 2;
/* result:
{
    plan: "table.Scan(\"test\") | rows.Filter(7 > a + 1)"
}
*/

-- test: precalculate BETWEEEN with path
EXPLAIN SELECT * FROM test WHERE 4 + 3 + a BETWEEN 3 + 6 AND 5 * 10;
/* result:
{
    plan: "table.Scan(\"test\") | rows.Filter(7 + a BETWEEN 9 AND 50)"
}
*/

-- test: precalculate BETWEEEN without path
EXPLAIN SELECT * FROM test WHERE 4 * 3 BETWEEN 3 + 6 AND 5 * 10;
/* result:
{
    plan: "table.Scan(\"test\")"
}
*/
