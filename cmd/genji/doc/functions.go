package doc

type functionDocs map[string]string

var packageDocs = map[string]functionDocs{
	"math": mathDocs,
	"":     builtinDocs,
}

var builtinDocs = functionDocs{
	"pk":    "The pk() function returns the primary key for the current document",
	"count": "Returns a count of the number of times that arg1 is not NULL in a group. The count(*) function (with no arguments) returns the total number of rows in the group.",
	"min":   "Returns the minimum value in a group.",
	"max":   "Returns the maximum value in a group.",
	"sum":   "The sum function returns the sum of all values in a group.",
	"avg":   "The avg function returns the average of all values in a group.",
}

var mathDocs = functionDocs{
	"floor": "Returns the greatest integer value less than or equal to arg1.",
}
