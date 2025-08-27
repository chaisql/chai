-- test: NO SUGAR
CREATE SEQUENCE seq NO SUGAR;
-- error:

-- test: duplicate AS INT
CREATE SEQUENCE seq AS INT AS INT;
-- error:

-- test: duplicate INCREMENT BY
CREATE SEQUENCE seq INCREMENT BY 10 INCREMENT BY 10;
-- error:

-- test: duplicate NO MINVALUE
CREATE SEQUENCE seq NO MINVALUE NO MINVALUE;
-- error:

-- test: MINVALUE > MAXVALUE (bad range)
CREATE SEQUENCE seq MINVALUE 10 MAXVALUE 5;
-- error:

-- test: START greater than MAX
CREATE SEQUENCE seq MINVALUE 5 MAXVALUE 10 START 100;
-- error:

-- test: START lower than MIN
CREATE SEQUENCE seq MINVALUE 5 MAXVALUE 10 START -100;
-- error:

-- test: missing name
CREATE SEQUENCE;
-- error:

-- test: IF NOT EXISTS but missing name
CREATE SEQUENCE IF NOT EXISTS;
-- error:

-- test: duplicate INCREMENT (mixed forms)
CREATE SEQUENCE seq INCREMENT BY 2 INCREMENT 2;
-- error:

-- test: duplicate MINVALUE
CREATE SEQUENCE seq MINVALUE 5 MINVALUE 6;
-- error:

-- test: duplicate MAXVALUE
CREATE SEQUENCE seq MAXVALUE 10 MAXVALUE 11;
-- error:

-- test: duplicate START
CREATE SEQUENCE seq START 1 START 2;
-- error:

-- test: duplicate CACHE
CREATE SEQUENCE seq CACHE 2 CACHE 3;
-- error:

-- test: duplicate CYCLE
CREATE SEQUENCE seq CYCLE CYCLE;
-- error:
