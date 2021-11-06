-- test: simple projection
SELECT 1;
/* result:
{"1": 1}
*/

-- test: complex expression
SELECT 1 + 1 * 2 / 4;
/* result:
{"1 + 1 * 2 / 4": 1}
*/

-- test: with spaces
SELECT     1  + 1 *      2 /                    4;
/* result:
{"1 + 1 * 2 / 4": 1}
*/

-- test: escaping, double quotes
SELECT '"A"';
/* result:
{`"\\"A\\""`: "\"A\""}
*/

-- test: escaping, single quotes
SELECT "'A'";
/* result:
{`"'A'"`: "'A'"}
*/

-- test: document
SELECT {a: 1, b: 2 + 1};
/* result:
{"{a: 1, b: 2 + 1}":{"a":1,"b":3}}
*/

-- test: aliases
SELECT 1 AS A;
/* result:
{"A": 1}
*/

-- test: aliases with cast
SELECT CAST(1 AS DOUBLE) AS A;
/* result:
{"A": 1.0}
*/

-- test: pk()
SELECT pk();
/* result:
{"pk()": null}
*/

-- test: field
SELECT a;
-- error:

-- test: wildcard
SELECT *;
-- error:

