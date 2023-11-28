package doc

type functionDocs map[string]string

var packageDocs = map[string]functionDocs{
	"strings": stringsDocs,
	"math":    mathDocs,
	"":        builtinDocs,
}

var builtinDocs = functionDocs{
	"pk":       "The pk() function returns the primary key for the current document",
	"count":    "Returns a count of the number of times that arg1 is not NULL in a group. The count(*) function (with no arguments) returns the total number of rows in the group.",
	"min":      "Returns the minimum value of the arg1 expression in a group.",
	"max":      "Returns the maximum value of the arg1 expressein in a group.",
	"sum":      "The sum function returns the sum of all values taken by the arg1 expression in a group.",
	"avg":      "The avg function returns the average of all values taken by the arg1 expression in a group.",
	"typeof":   "The typeof function returns the type of arg1.",
	"len":      "The len function returns length of the arg1 expression if arg1 evals to string, array or document, either returns NULL.",
	"coalesce": "The coalesce function returns the first non-null argument. NULL is returned if all arguments are null.",
}

var mathDocs = functionDocs{
	"abs":    "Returns the absolute value of arg1.",
	"acos":   "Returns the arcosine, in radiant, of arg1.",
	"acosh":  "Returns the inverse hyperbolic cosine of arg1.",
	"asin":   "Returns the arsine, in radiant, of arg1.",
	"asinh":  "Returns the inverse hyperbolic sine of arg1.",
	"atan":   "Returns the arctangent, in radians, of arg1.",
	"atan2":  "Returns the arctangent of arg1/arg2, using the signs of the two to determine the quadrant of the return value.",
	"floor":  "Returns the greatest integer value less than or equal to arg1.",
	"random": "The random function returns a random number between math.MinInt64 and math.MaxInt64.",
	"sqrt":   "The sqrt function returns the square root of arg1.",
}

var stringsDocs = functionDocs{
	"lower": "The lower function returns arg1 to lower-case if arg1 evals to string",
	"upper": "The upper function returns arg1 to upper-case if arg1 evals to string",
	"trim":  "The trim function returns arg1 with leading and trailing characters removed. space by default or arg2",
	"ltrim": "The ltrim function returns arg1 with leading characters removed. space by default or arg2",
	"rtrim": "The rtrim function returns arg1 with trailing characters removed. space by default or arg2",
}
