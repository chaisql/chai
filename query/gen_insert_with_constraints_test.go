/*
* CODE GENERATED AUTOMATICALLY WITH github.com/genjidb/genji/dev/gensqltest
* THIS FILE SHOULD NOT BE EDITED BY HAND
 */
package query_test

import (
	"testing"

	"github.com/genjidb/genji"
	"github.com/stretchr/testify/require"
)

func TestGenInsertWithConstraints(t *testing.T) {
	setup := func(t *testing.T, db *genji.DB) {
		t.Helper()

		q := `
`
		err := db.Exec(q)
		require.NoError(t, err)
	}

	// --------------------------------------------------------------------------
	t.Run("insert with errors, not null without type constraint", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a NOT NULL);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a NOT NULL);
INSERT INTO test_e VALUES {};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("insert with errors, array / not null with type constraint", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a ARRAY NOT NULL);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a ARRAY NOT NULL);
INSERT INTO test_e VALUES {};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("insert with errors, array / not null with non-respected type constraint", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a ARRAY NOT NULL);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a ARRAY NOT NULL);
INSERT INTO test_e VALUES {a: 42};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("insert with errors, blob", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a BLOB);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a BLOB);
INSERT INTO test_e {a: true};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("blob / not null with type constraint", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a BLOB NOT NULL);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a BLOB NOT NULL);
INSERT INTO test_e {};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("blob / not null with non-respected type constraint", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a BLOB NOT NULL);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a BLOB NOT NULL);
INSERT INTO test_e {a: 42};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("bool / not null with type constraint", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a BOOL NOT NULL);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a BOOL NOT NULL);
INSERT INTO test_e {};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("bytes", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a BYTES);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a BYTES);
INSERT INTO test_e {a: [1,2,3]};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("bytes / not null with type constraint", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a BYTES NOT NULL);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a BYTES NOT NULL);
INSERT INTO test_e {};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("bytes / not null with non-respected type constraint", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a BYTES NOT NULL);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a BYTES NOT NULL);
INSERT INTO test_e {a: 42};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("document", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a DOCUMENT);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a DOCUMENT);
INSERT INTO test_e {"a": "foo"};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("document / not null with type constraint", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a DOCUMENT NOT NULL);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a DOCUMENT NOT NULL);
INSERT INTO test_e {};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("document / not null with non-respected type constraint", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a DOCUMENT NOT NULL);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a DOCUMENT NOT NULL);
INSERT INTO test_e {a: false};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("double", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a DOUBLE);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a DOUBLE);
INSERT INTO test_e {a: "foo"};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("double / not null with type constraint", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a DOUBLE NOT NULL);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a DOUBLE NOT NULL);
INSERT INTO test_e {};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("double / not null with non-respected type constraint", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a DOUBLE NOT NULL);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a DOUBLE NOT NULL);
INSERT INTO test_e {a: [1,2,3]};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("double precision", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a DOUBLE PRECISION);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a DOUBLE PRECISION);
INSERT INTO test_e {a: "foo"};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("double precision / not null with type constraint", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a DOUBLE PRECISION NOT NULL);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a DOUBLE PRECISION NOT NULL);
INSERT INTO test_e {};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("double precision / not null with non-respected type constraint", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a DOUBLE PRECISION NOT NULL);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a DOUBLE PRECISION NOT NULL);
INSERT INTO test_e {a: [1,2,3]};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("real", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a REAL);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a REAL);
INSERT INTO test_e {a: "foo"};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("real / not null with type constraint", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a REAL NOT NULL);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a REAL NOT NULL);
INSERT INTO test_e {};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("real / not null with non-respected type constraint", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a REAL NOT NULL);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a REAL NOT NULL);
INSERT INTO test_e {a: [1,2,3]};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("integer", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a INTEGER);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a INTEGER);
INSERT INTO test_e {a: "foo"};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("integer / not null with type constraint", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a INTEGER NOT NULL);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a INTEGER NOT NULL);
INSERT INTO test_e {};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("integer / not null with non-respected type constraint", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a INTEGER NOT NULL);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a INTEGER NOT NULL);
INSERT INTO test_e {a: [1,2,3]};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("int2", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a INT2);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a INT2);
INSERT INTO test_e {a: "foo"};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("int2 / not null with type constraint", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a INT2 NOT NULL);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a INT2 NOT NULL);
INSERT INTO test_e {};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("int2 / not null with non-respected type constraint", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a INT NOT NULL);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a INT NOT NULL);
INSERT INTO test_e {a: [1,2,3]};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("int8", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a INT8);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a INT8);
INSERT INTO test_e {a: "foo"};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("int8 / not null with type constraint", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a INT8 NOT NULL);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a INT8 NOT NULL);
INSERT INTO test_e {};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("int8 / not null with non-respected type constraint", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a INT8 NOT NULL);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a INT8 NOT NULL);
INSERT INTO test_e {a: [1,2,3]};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("tinyint", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a TINYINT);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a TINYINT);
INSERT INTO test_e {a: "foo"};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("tinyint / not null with type constraint", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a TINYINT NOT NULL);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a TINYINT NOT NULL);
INSERT INTO test_e {};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("tinyint / not null with non-respected type constraint", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a TINYINT NOT NULL);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a TINYINT NOT NULL);
INSERT INTO test_e {a: [1,2,3]};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("bigint", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a BIGINT);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a BIGINT);
INSERT INTO test_e {a: "foo"};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("bigint / not null with type constraint", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a BIGINT NOT NULL);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a BIGINT NOT NULL);
INSERT INTO test_e {};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("bigint / not null with non-respected type constraint", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a BIGINT NOT NULL);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a BIGINT NOT NULL);
INSERT INTO test_e {a: [1,2,3]};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("smallint", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a SMALLINT);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a SMALLINT);
INSERT INTO test_e {a: "foo"};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("smallint / not null with type constraint", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a SMALLINT NOT NULL);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a SMALLINT NOT NULL);
INSERT INTO test_e {};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("smallint / not null with non-respected type constraint", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a SMALLINT NOT NULL);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a SMALLINT NOT NULL);
INSERT INTO test_e {a: [1,2,3]};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("mediumint", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a MEDIUMINT);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a MEDIUMINT);
INSERT INTO test_e {a: "foo"};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("mediumint / not null with type constraint", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a MEDIUMINT NOT NULL);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a MEDIUMINT NOT NULL);
INSERT INTO test_e {};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("mediumint / not null with non-respected type constraint", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a MEDIUMINT NOT NULL);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a MEDIUMINT NOT NULL);
INSERT INTO test_e {a: [1,2,3]};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("text / not null with type constraint", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a TEXT NOT NULL);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a TEXT NOT NULL);
INSERT INTO test_e {};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("varchar / not null with type constraint", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a VARCHAR(255) NOT NULL);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a VARCHAR(255) NOT NULL);
INSERT INTO test_e {};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

	// --------------------------------------------------------------------------
	t.Run("character / not null with type constraint", func(t *testing.T) {
		db, err := genji.Open(":memory:")
		require.NoError(t, err)
		defer db.Close()

		setup(t, db)

		t.Run(`CREATE TABLE test_e (a CHARACTER(64) NOT NULL);`, func(t *testing.T) {
			q := `
CREATE TABLE test_e (a CHARACTER(64) NOT NULL);
INSERT INTO test_e {};
`
			err := db.Exec(q)
			require.Errorf(t, err, "expected\n%s\nto raise an error but got none", q)
		})

	})

}
