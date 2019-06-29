package query_test

import (
	"log"

	"github.com/asdine/genji"
	"github.com/asdine/genji/query"
)

var tx *genji.Tx

func Example_Select() {
	// SELECT Name, Age FROM example WHERE Age >= 18
	res := query.
		Select(query.StringField("Name"), query.IntField("Age")).
		From(query.Table("example")).
		Where(query.IntField("Age").Gte(18)).
		Run(tx)

	if err := res.Err(); err != nil {
		log.Fatal(err)
	}
}

func Example_Insert() {
	// INSERT INTO example (Name, Age) VALUES ("foo", 21)
	res := query.
		Insert().
		Into(query.Table("example")).
		Fields("Name", "Age").
		Values(query.StringValue("foo"), query.IntValue(21)).
		Run(tx)

	if err := res.Err(); err != nil {
		log.Fatal(err)
	}
}
