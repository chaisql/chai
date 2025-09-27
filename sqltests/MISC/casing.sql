-- test: Identifiers are not case sensitive
CREATE TABLE Test(pk INT PRIMARY KEY);
INSERT INTO test (pk) VALUES (1) RETURNING *;
/* result:
{"pk": 1}
*/

-- test: Identifiers are not case sensitive
CREATE TABLE Test(pk INT PRIMARY KEY);
INSERT INTO tEst (pk) VALUES (1) RETURNING *;
/* result:
{"pk": 1}
*/

-- test: Identifiers are not case sensitive
CREATE TABLE TEST(pk INT PRIMARY KEY);
INSERT INTO test (pk) VALUES (1) RETURNING *;
/* result:
{"pk": 1}
*/
