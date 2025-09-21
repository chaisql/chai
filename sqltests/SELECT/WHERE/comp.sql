-- This file tests comparison operators with columns
-- of different types. It ensures the behavior (modulo ordering)
-- remains the same regardless of index usage.
-- It contains one test suite with no index and one test suite
-- per column where that particular column is indexed

-- setup:
CREATE TABLE test(
    id int primary key,
    a int,
    b double precision,
    c boolean,
    d text,
    e bytea
);

INSERT INTO test VALUES
    (1, 10, 1.0, false, 'a', '\xaa'),
    (2, 20, 2.0, true, 'b', '\xab'),
    (3, 30, 3.0, false, 'c', '\xac'),
    (4, 40, 4.0, true, 'd', '\xad');

-- suite: no index

-- suite: index on a
CREATE INDEX ON test(a);

-- suite: index on b
CREATE INDEX ON test(b);

-- suite: index on c
CREATE INDEX ON test(c);

-- suite: index on d
CREATE INDEX ON test(d);

-- suite: index on e
CREATE INDEX ON test(e);

-- test: pk =
SELECT * FROM test WHERE id = 1;
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: 'a',
        e: '\xaa'
    }
*/

-- test: pk !=
SELECT * FROM test WHERE id != 1;
/* result:
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: 'b',
        e: '\xab'
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: 'c',
        e: '\xac'
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: 'd',
        e: '\xad'
    }
*/

-- test: pk >
SELECT * FROM test WHERE id > 1;
/* result:
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: 'b',
        e: '\xab'
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: 'c',
        e: '\xac'
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
    d: 'd',
    e: '\xad'
    }
*/

-- test: pk >=
SELECT * FROM test WHERE id >= 1;
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: 'a',
        e: '\xaa'
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: 'b',
        e: '\xab'
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: 'c',
        e: '\xac'
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: 'd',
        e: '\xad'
    }
*/

-- test: pk <
SELECT * FROM test WHERE id < 3;
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: 'a',
        e: '\xaa'
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: 'b',
        e: '\xab'
    }
*/

-- test: pk <=
SELECT * FROM test WHERE id <= 3;
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: 'a',
        e: '\xaa'
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: 'b',
        e: '\xab'
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: 'c',
        e: '\xac'
    }
*/

-- test: pk IN
SELECT * FROM test WHERE id IN (1, 3);
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: 'a',
        e: '\xaa'
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: 'c',
        e: '\xac'
    }
*/

-- test: pk NOT IN
SELECT * FROM test WHERE id NOT IN (1, 3);
/* result:
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: 'b',
        e: '\xab'
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: 'd',
        e: '\xad'
    }
*/

-- test: int =
SELECT * FROM test WHERE a = 10;
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: 'a',
        e: '\xaa'
    }
*/

-- test: int !=
SELECT * FROM test WHERE a != 10;
/* result:
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: 'b',
        e: '\xab'
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: 'c',
        e: '\xac'
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: 'd',
        e: '\xad'
    }
*/

-- test: int >
SELECT * FROM test WHERE a > 10;
/* result:
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: 'b',
        e: '\xab'
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: 'c',
        e: '\xac'
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: 'd',
        e: '\xad'
    }
*/

-- test: int >=
SELECT * FROM test WHERE a >= 10;
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: 'a',
        e: '\xaa'
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: 'b',
        e: '\xab'
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: 'c',
        e: '\xac'
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: 'd',
        e: '\xad'
    }
*/

-- test: int <
SELECT * FROM test WHERE a < 30;
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: 'a',
        e: '\xaa'
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: 'b',
        e: '\xab'
    }
*/

-- test: int <=
SELECT * FROM test WHERE a <= 30;
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: 'a',
        e: '\xaa'
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: 'b',
        e: '\xab'
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: 'c',
        e: '\xac'
    }
*/

-- test: int IN
SELECT * FROM test WHERE a IN (10, 30);
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: 'a',
        e: '\xaa'
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: 'c',
        e: '\xac'
    }
*/

-- test: int NOT IN
SELECT * FROM test WHERE a NOT IN (10, 30);
/* result:
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: 'b',
        e: '\xab'
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: 'd',
        e: '\xad'
    }
*/

-- test: double =
SELECT * FROM test WHERE b = 1.0;
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: 'a',
        e: '\xaa'
    }
*/

-- test: double !=
SELECT * FROM test WHERE b != 1.0;
/* result:
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: 'b',
        e: '\xab'
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: 'c',
        e: '\xac'
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: 'd',
        e: '\xad'
    }
*/

-- test: double >
SELECT * FROM test WHERE b > 1.0;
/* result:
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: 'b',
        e: '\xab'
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: 'c',
        e: '\xac'
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: 'd',
        e: '\xad'
    }
*/

-- test: double >=
SELECT * FROM test WHERE b >= 1.0;
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: 'a',
        e: '\xaa'
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: 'b',
        e: '\xab'
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: 'c',
        e: '\xac'
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: 'd',
        e: '\xad'
    }
*/

-- test: double <
SELECT * FROM test WHERE b < 3.0;
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: 'a',
        e: '\xaa'
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: 'b',
        e: '\xab'
    }
*/

-- test: double <=
SELECT * FROM test WHERE b <= 3.0;
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: 'a',
        e: '\xaa'
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: 'b',
        e: '\xab'
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: 'c',
        e: '\xac'
    }
*/

-- test: double IN
SELECT * FROM test WHERE b IN (1.0, 3.0);
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: 'a',
        e: '\xaa'
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: 'c',
        e: '\xac'
    }
*/

-- test: double NOT IN
SELECT * FROM test WHERE b NOT IN (1.0, 3.0);
/* result:
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: 'b',
        e: '\xab'
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: 'd',
        e: '\xad'
    }
*/

-- test: bool =
SELECT * FROM test WHERE c = true;
/* result:
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: 'b',
        e: '\xab'
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: 'd',
        e: '\xad'
    }
*/

-- test: bool !=
SELECT * FROM test WHERE c != true;
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: 'a',
        e: '\xaa'
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: 'c',
        e: '\xac'
    }
*/

-- test: bool >
SELECT * FROM test WHERE c > false;
/* result:
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: 'b',
        e: '\xab'
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: 'd',
        e: '\xad'
    }
*/

-- test: bool >=
SELECT * FROM test WHERE c >= false ORDER BY id;
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: 'a',
        e: '\xaa'
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: 'b',
        e: '\xab'
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: 'c',
        e: '\xac'
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: 'd',
        e: '\xad'
    }
*/

-- test: bool <
SELECT * FROM test WHERE c < true ORDER BY id;
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: 'a',
        e: '\xaa'
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: 'c',
        e: '\xac'
    }
*/

-- test: bool <=
SELECT * FROM test WHERE c <= true ORDER BY id;
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: 'a',
        e: '\xaa'
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: 'b',
        e: '\xab'
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: 'c',
        e: '\xac'
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: 'd',
        e: '\xad'
    }
*/

-- test: bool IN
SELECT * FROM test WHERE c IN (true, false) ORDER BY id;
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: 'a',
        e: '\xaa'
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: 'b',
        e: '\xab'
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: 'c',
        e: '\xac'
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: 'd',
        e: '\xad'
    }
*/

-- test: bool NOT IN
SELECT * FROM test WHERE c NOT IN (true, 3);
-- error:

-- test: text =
SELECT * FROM test WHERE d = 'a';
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: 'a',
        e: '\xaa'
    }
*/

-- test: text !=
SELECT * FROM test WHERE d != 'a';
/* result:
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: 'b',
        e: '\xab'
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: 'c',
        e: '\xac'
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: 'd',
        e: '\xad'
    }
*/

-- test: text >
SELECT * FROM test WHERE d > 'a';
/* result:
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: 'b',
        e: '\xab'
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: 'c',
        e: '\xac'
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: 'd',
        e: '\xad'
    }
*/

-- test: text >=
SELECT * FROM test WHERE d >= 'a';
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: 'a',
        e: '\xaa'
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: 'b',
        e: '\xab'
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: 'c',
        e: '\xac'
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: 'd',
        e: '\xad'
    }
*/

-- test: text <
SELECT * FROM test WHERE d < 'c';
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: 'a',
        e: '\xaa'
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: 'b',
        e: '\xab'
    }
*/

-- test: text <=
SELECT * FROM test WHERE d <= 'c';
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: 'a',
        e: '\xaa'
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: 'b',
        e: '\xab'
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: 'c',
        e: '\xac'
    }
*/

-- test: text IN
SELECT * FROM test WHERE d IN ('a', 'c');
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: 'a',
        e: '\xaa'
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: 'c',
        e: '\xac'
    }
*/

-- test: text NOT IN
SELECT * FROM test WHERE d NOT IN ('a', 'c');
/* result:
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: 'b',
        e: '\xab'
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: 'd',
        e: '\xad'
    }
*/

-- test: bytea =
SELECT * FROM test WHERE e = '\xaa';
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: 'a',
        e: '\xaa'
    }
*/

-- test: bytea !=
SELECT * FROM test WHERE e != '\xaa';
/* result:
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: 'b',
        e: '\xab'
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: 'c',
        e: '\xac'
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: 'd',
        e: '\xad'
    }
*/

-- test: bytea >
SELECT * FROM test WHERE e > '\xaa';
/* result:
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: 'b',
        e: '\xab'
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: 'c',
        e: '\xac'
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: 'd',
        e: '\xad'
    }
*/

-- test: bytea >=
SELECT * FROM test WHERE e >= '\xaa';
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: 'a',
        e: '\xaa'
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: 'b',
        e: '\xab'
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: 'c',
        e: '\xac'
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: 'd',
        e: '\xad'
    }
*/

-- test: bytea <
SELECT * FROM test WHERE e < '\xac';
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: 'a',
        e: '\xaa'
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: 'b',
        e: '\xab'
    }
*/

-- test: bytea <=
SELECT * FROM test WHERE e <= '\xac';
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: 'a',
        e: '\xaa'
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: 'b',
        e: '\xab'
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: 'c',
        e: '\xac'
    }
*/

-- test: bytea IN
SELECT * FROM test WHERE e IN ('\xaa', '\xac');
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: 'a',
        e: '\xaa'
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: 'c',
        e: '\xac'
    }
*/

-- test: bytea NOT IN
SELECT * FROM test WHERE e NOT IN ('\xaa', '\xac');
/* result:
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: 'b',
        e: '\xab'
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: 'd',
        e: '\xad'
    }
*/




