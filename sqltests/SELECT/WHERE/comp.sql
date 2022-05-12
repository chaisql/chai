-- This file tests comparison operators with field paths
-- of different types. It ensures the behavior (modulo ordering)
-- remains the same regardless of index usage.
-- It contains one test suite with no index and one test suite
-- per field where that particular field is indexed

-- setup:
CREATE TABLE test(
    id int primary key,
    a int,
    b double,
    c boolean,
    d text,
    e blob,
    f (a int), -- f document
    g ARRAY -- g array
);

INSERT INTO test VALUES
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    },
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    },
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
    },
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4.0]
    };

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

-- suite: index on f
CREATE INDEX ON test(f);

-- suite: index on g
CREATE INDEX ON test(g);

-- test: pk =
SELECT * FROM test WHERE id = 1;
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
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
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4.0]
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
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4.0]
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
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4.0]
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
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
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
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
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
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
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
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4.0]
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
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
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
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4.0]
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
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4.0]
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
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4.0]
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
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
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
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
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
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
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
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4.0]
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
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
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
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4.0]
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
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4.0]
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
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4.0]
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
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
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
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
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
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
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
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4.0]
    }
*/

-- test: bool =
SELECT * FROM test WHERE c = true;
/* sorted-result:
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4.0]
    }
*/

-- test: bool !=
SELECT * FROM test WHERE c != true;
/* sorted-result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
    }
*/

-- test: bool >
SELECT * FROM test WHERE c > false;
/* sorted-result:
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4.0]
    }
*/

-- test: bool >=
SELECT * FROM test WHERE c >= false;
/* sorted-result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4.0]
    }
*/

-- test: bool <
SELECT * FROM test WHERE c < true;
/* sorted-result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
    }
*/

-- test: bool <=
SELECT * FROM test WHERE c <= true;
/* sorted-result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4.0]
    }
*/

-- test: bool IN
SELECT * FROM test WHERE c IN (true, false);
/* sorted-result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4.0]
    }
*/

-- test: bool NOT IN
SELECT * FROM test WHERE c NOT IN (true, 3);
/* sorted-result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
    }
*/

-- test: text =
SELECT * FROM test WHERE d = "a";
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    }
*/

-- test: text !=
SELECT * FROM test WHERE d != "a";
/* result:
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4.0]
    }
*/

-- test: text >
SELECT * FROM test WHERE d > "a";
/* result:
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4.0]
    }
*/

-- test: text >=
SELECT * FROM test WHERE d >= "a";
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4.0]
    }
*/

-- test: text <
SELECT * FROM test WHERE d < "c";
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
*/

-- test: text <=
SELECT * FROM test WHERE d <= "c";
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
    }
*/

-- test: text IN
SELECT * FROM test WHERE d IN ("a", "c");
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
    }
*/

-- test: text NOT IN
SELECT * FROM test WHERE d NOT IN ("a", "c");
/* result:
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4.0]
    }
*/

-- test: blob =
SELECT * FROM test WHERE e = "\xaa";
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    }
*/

-- test: blob !=
SELECT * FROM test WHERE e != "\xaa";
/* result:
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4.0]
    }
*/

-- test: blob >
SELECT * FROM test WHERE e > "\xaa";
/* result:
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4.0]
    }
*/

-- test: blob >=
SELECT * FROM test WHERE e >= "\xaa";
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4.0]
    }
*/

-- test: blob <
SELECT * FROM test WHERE e < "\xac";
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
*/

-- test: blob <=
SELECT * FROM test WHERE e <= "\xac";
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
    }
*/

-- test: blob IN
SELECT * FROM test WHERE e IN ("\xaa", "\xac");
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
    }
*/

-- test: blob NOT IN
SELECT * FROM test WHERE e NOT IN ("\xaa", "\xac");
/* result:
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4.0]
    }
*/

-- test: doc =
SELECT * FROM test WHERE f = {a: 1};
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    }
*/

-- test: doc !=
SELECT * FROM test WHERE f != {a: 1};
/* result:
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4.0]
    }
*/

-- test: doc >
SELECT * FROM test WHERE f > {a: 1};
/* result:
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4.0]
    }
*/

-- test: doc >=
SELECT * FROM test WHERE f >= {a: 1};
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4.0]
    }
*/

-- test: doc <
SELECT * FROM test WHERE f < {a: 3};
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
*/

-- test: doc <=
SELECT * FROM test WHERE f <= {a: 3};
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
    }
*/

-- test: doc IN
SELECT * FROM test WHERE f IN ({a: 1}, {a: 3});
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
    }
*/

-- test: doc NOT IN
SELECT * FROM test WHERE f NOT IN ({a: 1}, {a: 3});
/* result:
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4.0]
    }
*/

-- test: array =
SELECT * FROM test WHERE g = [1];
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    }
*/

-- test: array !=
SELECT * FROM test WHERE g != [1];
/* result:
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4.0]
    }
*/

-- test: array >
SELECT * FROM test WHERE g > [1];
/* result:
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4.0]
    }
*/

-- test: array >=
SELECT * FROM test WHERE g >= [1];
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4.0]
    }
*/

-- test: array <
SELECT * FROM test WHERE g < [3];
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
*/

-- test: array <=
SELECT * FROM test WHERE g <= [3];
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    }
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
    }
*/

-- test: array IN
SELECT * FROM test WHERE g IN ([1], [3]);
/* result:
    {
        id: 1,
        a: 10,
        b: 1.0,
        c: false,
        d: "a",
        e: "\xaa",
        f: {a: 1},
        g: [1.0]
    }
    {
        id: 3,
        a: 30,
        b: 3.0,
        c: false,
        d: "c",
        e: "\xac",
        f: {a: 3},
        g: [3.0]
    }
*/

-- test: array NOT IN
SELECT * FROM test WHERE g NOT IN ([1], [3]);
/* result:
    {
        id: 2,
        a: 20,
        b: 2.0,
        c: true,
        d: "b",
        e: "\xab",
        f: {a: 2},
        g: [2.0]
    }
    {
        id: 4,
        a: 40,
        b: 4.0,
        c: true,
        d: "d",
        e: "\xad",
        f: {a: 4},
        g: [4.0]
    }
*/