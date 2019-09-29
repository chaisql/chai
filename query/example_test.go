package query_test

import (
	"log"

	"github.com/asdine/genji/database"
	"github.com/asdine/genji/query"
)

var tx *database.Tx

func ExampleSelect() {
	// SELECT Name, Age FROM example WHERE Age >= 18
	res := query.
		Select().
		From(query.Table("example")).
		Where(query.IntField("Age").Gte(18)).
		Exec(tx)

	if err := res.Err(); err != nil {
		log.Fatal(err)
	}
}

func ExampleAnd() {
	// SELECT Name, Age FROM example WHERE Age >= 18 AND Age < 100
	res := query.
		Select().
		From(query.Table("example")).
		Where(
			query.And(
				query.IntField("Age").Gte(18),
				query.IntField("Age").Lt(100),
			),
		).
		Exec(tx)

	if err := res.Err(); err != nil {
		log.Fatal(err)
	}
}

func ExampleOr() {
	// SELECT Name, Age FROM example WHERE Age >= 18 OR Group = "staff"
	res := query.
		Select().
		From(query.Table("example")).
		Where(
			query.Or(
				query.IntField("Age").Gte(18),
				query.StringField("Age").Eq("staff"),
			),
		).
		Exec(tx)

	if err := res.Err(); err != nil {
		log.Fatal(err)
	}
}

func ExampleInsert() {
	// INSERT INTO example (Name, Age) VALUES ("foo", 21)
	res := query.
		Insert().
		Into(query.Table("example")).
		Fields("Name", "Age").
		Values(query.StringValue("foo"), query.IntValue(21)).
		Exec(tx)

	if err := res.Err(); err != nil {
		log.Fatal(err)
	}
}

func ExampleDelete() {
	// DELETE FROM example (Name, Age) WHERE Age >= 18
	res := query.
		Delete().
		From(query.Table("example")).
		Where(query.IntField("Age").Gte(18)).
		Exec(tx)

	if res.Err() != nil {
		log.Fatal(res.Err())
	}
}
