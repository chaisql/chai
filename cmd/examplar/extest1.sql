--- setup:
CREATE TABLE foo (a int);

--- test: insert something
INSERT INTO foo (a) VALUES (1);
SELECT * FROM foo;
--- `[{"a": 1}]`

SELECT a, b FROM foo;
--- ```json
--- [{
---   "a": 1,
---   "b": null
--- }]
--- ```

SELECT z FROM foo;
--- `[{"z": null}]`

--- test: something else
INSERT INTO foo (c) VALUES (3);
SELECT * FROM foo;
--- `[{"c": 3}]`
