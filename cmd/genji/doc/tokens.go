package doc

import "github.com/genjidb/genji/internal/sql/scanner"

var tokenDocs map[scanner.Token]string

func init() {
	tokenDocs = make(map[scanner.Token]string)
	// let's make sure the doc doesn't suggest that a keyword doesn't exist because
	// it has no doc defined.
	for _, tok := range scanner.AllKeywords() {
		tokenDocs[tok] = "TODO"
	}

	tokenDocs[scanner.BY] = "See GROUP BY, ORDER BY"
	tokenDocs[scanner.FROM] = "FROM [TABLE] selects documents in the table named [TABLE]"
}
