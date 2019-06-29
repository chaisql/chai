// Package query provides a type safe SQL like API using Go types and functions.
// This API can be run within a Genji transaction to query or alter the database.
// The typical use is to run the provided DSLs for each of the most common SQL
// commands.
// Each DSL is thread safe and can be reused in or out of a transaction using its Run method.
//
// Select:
//   query.Select().From(...).Where(...).Run(tx)
// Insert:
//   query.Insert().Into(...).Fields(...).Values(...).Run(tx)
// Delete:
//   query.Delete().From(...).Where(...).Run(tx)
package query
