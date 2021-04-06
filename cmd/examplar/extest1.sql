--- setup:
CREATE TABLE foo (a int);

--- teardown:
DROP TABLE foo;

--- test: insert something
INSERT INTO foo (1);
--- `1`

