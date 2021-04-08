--- setup:
CREATE TABLE foo (a int);

--- teardown:
DROP TABLE foo;

--- test: insert something
INSERT INTO foo (a) VALUES (1);
SELECT * FROM foo;
--- `{"a": 1}`

SELECT a, b FROM foo;
--- ```json
--- {
---   "a": 1,
---   "b": null
--- }
--- ```
