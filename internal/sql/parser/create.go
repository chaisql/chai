package parser

import (
	"fmt"
	"math"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/query/statement"
	"github.com/genjidb/genji/internal/sql/scanner"
	"github.com/genjidb/genji/types"
)

// parseCreateStatement parses a create string and returns a Statement AST object.
func (p *Parser) parseCreateStatement() (statement.Statement, error) {
	// Parse "CREATE".
	if err := p.parseTokens(scanner.CREATE); err != nil {
		return nil, err
	}

	tok, pos, lit := p.ScanIgnoreWhitespace()
	switch tok {
	case scanner.TABLE:
		return p.parseCreateTableStatement()
	case scanner.UNIQUE:
		if tok, pos, lit := p.ScanIgnoreWhitespace(); tok != scanner.INDEX {
			return nil, newParseError(scanner.Tokstr(tok, lit), []string{"INDEX"}, pos)
		}

		return p.parseCreateIndexStatement(true)
	case scanner.INDEX:
		return p.parseCreateIndexStatement(false)
	case scanner.SEQUENCE:
		return p.parseCreateSequenceStatement()
	}

	return nil, newParseError(scanner.Tokstr(tok, lit), []string{"TABLE", "INDEX", "SEQUENCE"}, pos)
}

// parseCreateTableStatement parses a create table string and returns a Statement AST object.
// This function assumes the CREATE TABLE tokens have already been consumed.
func (p *Parser) parseCreateTableStatement() (*statement.CreateTableStmt, error) {
	var stmt statement.CreateTableStmt
	var err error

	// Parse IF NOT EXISTS
	stmt.IfNotExists, err = p.parseOptional(scanner.IF, scanner.NOT, scanner.EXISTS)
	if err != nil {
		return nil, err
	}

	// Parse table name
	stmt.Info.TableName, err = p.parseIdent()
	if err != nil {
		return nil, err
	}

	// parse field constraints
	err = p.parseConstraints(&stmt)
	if err != nil {
		return nil, err
	}

	if len(stmt.Info.FieldConstraints.Ordered) == 0 {
		stmt.Info.FieldConstraints.AllowExtraFields = true
	}
	return &stmt, err
}

func (p *Parser) parseConstraints(stmt *statement.CreateTableStmt) error {
	// Parse ( token.
	if ok, err := p.parseOptional(scanner.LPAREN); !ok || err != nil {
		return err
	}

	// if set to true, the parser must no longer
	// expect field definitions, but only table constraints.
	var parsingTableConstraints bool

	stmt.Info.FieldConstraints, _ = database.NewFieldConstraints()

	var allTableConstraints []*database.TableConstraint

	// Parse constraints.
	for {
		// start with the ellipsis token.
		// if found, stop parsing constraints, as it should be the last one.
		tok, _, _ := p.ScanIgnoreWhitespace()
		if tok == scanner.ELLIPSIS {
			stmt.Info.FieldConstraints.AllowExtraFields = true
			break
		}
		p.Unscan()

		// then we check if it is a table constraint,
		// as it's easier to determine
		tc, err := p.parseTableConstraint(stmt)
		if err != nil {
			return err
		}

		// no table constraint found
		if tc == nil && parsingTableConstraints {
			tok, pos, lit := p.ScanIgnoreWhitespace()
			return newParseError(scanner.Tokstr(tok, lit), []string{"CONSTRAINT", ")"}, pos)
		}

		if tc != nil {
			parsingTableConstraints = true
			allTableConstraints = append(allTableConstraints, tc)
		}

		// if set to false, we are still parsing field definitions
		if !parsingTableConstraints {
			fc, tcs, err := p.parseFieldDefinition(document.Path{})
			if err != nil {
				return err
			}

			err = stmt.Info.AddFieldConstraint(fc)
			if err != nil {
				return err
			}

			allTableConstraints = append(allTableConstraints, tcs...)
		}

		if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.COMMA {
			p.Unscan()
			break
		}
	}

	// Parse required ) token.
	if err := p.parseTokens(scanner.RPAREN); err != nil {
		return err
	}

	// add all table constraints to the table info
	for _, tc := range allTableConstraints {
		err := stmt.Info.AddTableConstraint(tc)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *Parser) parseFieldDefinition(parent document.Path) (*database.FieldConstraint, []*database.TableConstraint, error) {
	var err error

	var fc database.FieldConstraint

	fc.Field, err = p.parseIdent()
	if err != nil {
		return nil, nil, err
	}

	fc.Type, err = p.parseType()
	if err != nil {
		p.Unscan()
	}

	path := parent.ExtendField(fc.Field)

	var tcs []*database.TableConstraint

	if fc.Type.IsAny() || fc.Type == types.DocumentValue {
		anon, nestedTCs, err := p.parseDocumentDefinition(path)
		if err != nil {
			return nil, nil, err
		}
		if anon != nil {
			fc.Type = types.DocumentValue
			fc.AnonymousType = anon
		} else if fc.Type == types.DocumentValue {
			// if the field constraint is a document but doesn't have any constraint,
			// its AllowExtraFields is set to true
			// i.e CREATE TABLE foo(a DOCUMENT) -> CREATE TABLE foo(a DOCUMENT (...))
			fc.AnonymousType = &database.AnonymousType{}
			fc.AnonymousType.FieldConstraints.AllowExtraFields = true
		}

		if len(nestedTCs) != 0 {
			tcs = append(tcs, nestedTCs...)
		}
	}

LOOP:
	for {
		tok, pos, lit := p.ScanIgnoreWhitespace()
		switch tok {
		case scanner.PRIMARY:
			// Parse "KEY"
			if err := p.parseTokens(scanner.KEY); err != nil {
				return nil, nil, err
			}

			tcs = append(tcs, &database.TableConstraint{
				PrimaryKey: true,
				Paths:      document.Paths{path},
			})
		case scanner.NOT:
			// Parse "NULL"
			if err := p.parseTokens(scanner.NULL); err != nil {
				return nil, nil, err
			}

			// if it's already not null we return an error
			if fc.IsNotNull {
				return nil, nil, newParseError(scanner.Tokstr(tok, lit), []string{"CONSTRAINT", ")"}, pos)
			}

			fc.IsNotNull = true
		case scanner.DEFAULT:
			// if it has already a default value we return an error
			if fc.HasDefaultValue() {
				return nil, nil, newParseError(scanner.Tokstr(tok, lit), []string{"CONSTRAINT", ")"}, pos)
			}

			withParentheses, err := p.parseOptional(scanner.LPAREN)
			if err != nil {
				return nil, nil, err
			}

			// Parse default value expression.
			// Only a few tokens are allowed.
			e, err := p.parseExprWithMinPrecedence(scanner.EQ.Precedence(),
				scanner.EQ,
				scanner.NEQ,
				scanner.BITWISEOR,
				scanner.BITWISEXOR,
				scanner.BITWISEAND,
				scanner.LT,
				scanner.LTE,
				scanner.GT,
				scanner.GTE,
				scanner.ADD,
				scanner.SUB,
				scanner.MUL,
				scanner.DIV,
				scanner.MOD,
				scanner.CONCAT,
				scanner.INTEGER,
				scanner.NUMBER,
				scanner.STRING,
				scanner.TRUE,
				scanner.FALSE,
				scanner.NULL,
				scanner.LPAREN,   // only opening parenthesis are necessary
				scanner.LBRACKET, // only opening brackets are necessary
				scanner.NEXT,
			)
			if err != nil {
				return nil, nil, err
			}

			fc.DefaultValue = expr.Constraint(e)

			if withParentheses {
				_, err = p.parseOptional(scanner.RPAREN)
				if err != nil {
					return nil, nil, err
				}
			}
		case scanner.UNIQUE:
			tcs = append(tcs, &database.TableConstraint{
				Unique: true,
				Paths:  document.Paths{path},
			})
		case scanner.CHECK:
			e, paths, err := p.parseCheckConstraint()
			if err != nil {
				return nil, nil, err
			}

			tcs = append(tcs, &database.TableConstraint{
				Check: expr.Constraint(e),
				Paths: paths,
			})
		default:
			p.Unscan()
			break LOOP
		}
	}

	return &fc, tcs, nil
}

func (p *Parser) parseDocumentDefinition(parent document.Path) (*database.AnonymousType, []*database.TableConstraint, error) {
	err := p.parseTokens(scanner.LPAREN)
	if err != nil {
		p.Unscan()
		return nil, nil, nil
	}

	var anon database.AnonymousType
	var nestedTcs []*database.TableConstraint

	for {
		// start with the ellipsis token.
		// if found, stop parsing constraints, as it should be the last one.
		tok, _, _ := p.ScanIgnoreWhitespace()
		if tok == scanner.ELLIPSIS {
			anon.FieldConstraints.AllowExtraFields = true
			break
		}
		p.Unscan()

		fc, tcs, err := p.parseFieldDefinition(parent)
		if err != nil {
			return nil, nil, err
		}

		err = anon.AddFieldConstraint(fc)
		if err != nil {
			return nil, nil, err
		}

		nestedTcs = append(nestedTcs, tcs...)

		if tok, _, _ := p.ScanIgnoreWhitespace(); tok != scanner.COMMA {
			p.Unscan()
			break
		}
	}

	err = p.parseTokens(scanner.RPAREN)
	if err != nil {
		return nil, nil, err
	}

	return &anon, nestedTcs, nil
}

func (p *Parser) parseTableConstraint(stmt *statement.CreateTableStmt) (*database.TableConstraint, error) {
	var err error

	var tc database.TableConstraint
	var requiresTc bool

	if ok, _ := p.parseOptional(scanner.CONSTRAINT); ok {
		tok, pos, lit := p.ScanIgnoreWhitespace()
		switch tok {
		case scanner.IDENT, scanner.STRING:
			tc.Name = lit
		default:
			return nil, newParseError(scanner.Tokstr(tok, lit), []string{"IDENT", "STRING"}, pos)
		}

		requiresTc = true
	}

	tok, pos, lit := p.ScanIgnoreWhitespace()
	switch tok {
	case scanner.PRIMARY:
		// Parse "KEY ("
		err = p.parseTokens(scanner.KEY)
		if err != nil {
			return nil, err
		}

		tc.PrimaryKey = true

		tc.Paths, err = p.parsePathList()
		if err != nil {
			return nil, err
		}
		if len(tc.Paths) == 0 {
			tok, pos, lit := p.ScanIgnoreWhitespace()
			return nil, newParseError(scanner.Tokstr(tok, lit), []string{"PATHS"}, pos)
		}
	case scanner.UNIQUE:
		tc.Unique = true
		tc.Paths, err = p.parsePathList()
		if err != nil {
			return nil, err
		}
		if len(tc.Paths) == 0 {
			tok, pos, lit := p.ScanIgnoreWhitespace()
			return nil, newParseError(scanner.Tokstr(tok, lit), []string{"PATHS"}, pos)
		}
	case scanner.CHECK:
		e, paths, err := p.parseCheckConstraint()
		if err != nil {
			return nil, err
		}

		tc.Check = expr.Constraint(e)
		tc.Paths = paths
	default:
		if requiresTc {
			return nil, newParseError(scanner.Tokstr(tok, lit), []string{"PRIMARY", "UNIQUE", "CHECK"}, pos)
		}

		p.Unscan()
		return nil, nil
	}

	return &tc, nil
}

// parseCreateIndexStatement parses a create index string and returns a Statement AST object.
// This function assumes the CREATE INDEX or CREATE UNIQUE INDEX tokens have already been consumed.
func (p *Parser) parseCreateIndexStatement(unique bool) (*statement.CreateIndexStmt, error) {
	var err error
	var stmt statement.CreateIndexStmt
	stmt.Info.Unique = unique

	// Parse IF NOT EXISTS
	stmt.IfNotExists, err = p.parseOptional(scanner.IF, scanner.NOT, scanner.EXISTS)
	if err != nil {
		return nil, err
	}

	// Parse optional index name
	stmt.Info.IndexName, err = p.parseIdent()
	if err != nil {
		// if IF NOT EXISTS is set, index name is mandatory
		if stmt.IfNotExists {
			return nil, err
		}

		p.Unscan()
	}

	// Parse "ON"
	if err := p.parseTokens(scanner.ON); err != nil {
		return nil, err
	}

	// Parse table name
	stmt.Info.Owner.TableName, err = p.parseIdent()
	if err != nil {
		return nil, err
	}

	paths, err := p.parsePathList()
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		tok, pos, lit := p.ScanIgnoreWhitespace()
		return nil, newParseError(scanner.Tokstr(tok, lit), []string{"("}, pos)
	}

	stmt.Info.Paths = paths

	return &stmt, nil
}

// This function assumes the CREATE SEQUENCE tokens have already been consumed.
func (p *Parser) parseCreateSequenceStatement() (*statement.CreateSequenceStmt, error) {
	var stmt statement.CreateSequenceStmt
	var err error

	// Parse IF NOT EXISTS
	stmt.IfNotExists, err = p.parseOptional(scanner.IF, scanner.NOT, scanner.EXISTS)
	if err != nil {
		return nil, err
	}

	// Parse sequence name
	stmt.Info.Name, err = p.parseIdent()
	if err != nil {
		return nil, err
	}

	var hasAsInt, hasNoMin, hasNoMax, hasNoCycle bool
	var min, max, incrementBy, start, cache *int64

	for {
		// Parse AS [any int type]
		// Only integers are supported
		if ok, _ := p.parseOptional(scanner.AS); ok {
			tok, pos, lit := p.ScanIgnoreWhitespace()
			switch tok {
			case scanner.TYPEINTEGER, scanner.TYPEINT, scanner.TYPEINT2, scanner.TYPEINT8, scanner.TYPETINYINT,
				scanner.TYPEBIGINT, scanner.TYPEMEDIUMINT, scanner.TYPESMALLINT:
			default:
				return nil, newParseError(scanner.Tokstr(tok, lit), []string{"INT"}, pos)
			}

			if hasAsInt {
				return nil, &ParseError{Message: "conflicting or redundant options"}
			}

			hasAsInt = true
			continue
		}

		// Parse INCREMENT [BY] integer
		if ok, _ := p.parseOptional(scanner.INCREMENT); ok {
			// parse optional BY token
			_, _ = p.parseOptional(scanner.BY)

			if incrementBy != nil {
				return nil, &ParseError{Message: "conflicting or redundant options"}
			}

			i, err := p.parseInteger()
			if err != nil {
				return nil, err
			}
			if i == 0 {
				return nil, &ParseError{Message: "INCREMENT must not be zero"}
			}
			incrementBy = &i

			continue
		}

		// Parse NO [MINVALUE | MAXVALUE | CYCLE]
		if ok, _ := p.parseOptional(scanner.NO); ok {
			tok, pos, lit := p.ScanIgnoreWhitespace()

			if tok == scanner.MINVALUE {
				if hasNoMin {
					return nil, &ParseError{Message: "conflicting or redundant options"}
				}
				hasNoMin = true
				continue
			}

			if tok == scanner.MAXVALUE {
				if hasNoMax {
					return nil, &ParseError{Message: "conflicting or redundant options"}
				}
				hasNoMax = true
				continue
			}

			if tok == scanner.CYCLE {
				if hasNoCycle {
					return nil, &ParseError{Message: "conflicting or redundant options"}
				}
				hasNoCycle = true
				continue
			}

			return nil, newParseError(scanner.Tokstr(tok, lit), []string{"MINVALUE", "MAXVALUE", "CYCLE"}, pos)
		}

		// Parse MINVALUE integer
		if ok, _ := p.parseOptional(scanner.MINVALUE); ok {
			if hasNoMin || min != nil {
				return nil, &ParseError{Message: "conflicting or redundant options"}
			}
			i, err := p.parseInteger()
			if err != nil {
				return nil, err
			}
			min = &i
			continue
		}

		// Parse MAXVALUE integer
		if ok, _ := p.parseOptional(scanner.MAXVALUE); ok {
			if hasNoMax || max != nil {
				return nil, &ParseError{Message: "conflicting or redundant options"}
			}
			i, err := p.parseInteger()
			if err != nil {
				return nil, err
			}
			max = &i
			continue
		}

		// Parse START [WITH] integer
		if ok, _ := p.parseOptional(scanner.START); ok {
			// parse optional WITH token
			_, _ = p.parseOptional(scanner.WITH)

			if start != nil {
				return nil, &ParseError{Message: "conflicting or redundant options"}
			}

			i, err := p.parseInteger()
			if err != nil {
				return nil, err
			}
			start = &i
			continue
		}

		// Parse CACHE integer
		if ok, _ := p.parseOptional(scanner.CACHE); ok {
			if cache != nil {
				return nil, &ParseError{Message: "conflicting or redundant options"}
			}

			v, err := p.parseInteger()
			if err != nil {
				return nil, err
			}
			if v < 0 {
				return nil, &ParseError{Message: "cache value must be positive"}
			}
			cache = &v

			continue
		}

		// Parse CYCLE
		if ok, _ := p.parseOptional(scanner.CYCLE); ok {
			if hasNoCycle || stmt.Info.Cycle {
				return nil, &ParseError{Message: "conflicting or redundant options"}
			}

			stmt.Info.Cycle = true
			continue
		}

		break
	}

	// default value for increment is 1
	if incrementBy != nil {
		stmt.Info.IncrementBy = *incrementBy
	} else {
		stmt.Info.IncrementBy = 1
	}

	// determine if the sequence is ascending or descending
	asc := stmt.Info.IncrementBy > 0

	// default value for min is 1 if ascending
	// or the minimum value of ints if descending
	if min != nil {
		stmt.Info.Min = *min
	} else if asc {
		stmt.Info.Min = 1
	} else {
		stmt.Info.Min = math.MinInt64
	}

	// default value for max is the maximum value of ints if ascending
	// or the -1 if descending
	if max != nil {
		stmt.Info.Max = *max
	} else if asc {
		stmt.Info.Max = math.MaxInt64
	} else {
		stmt.Info.Max = -1
	}

	// check if min > max
	if stmt.Info.Min > stmt.Info.Max {
		return nil, &ParseError{Message: fmt.Sprintf("MINVALUE (%d) must be less than MAXVALUE (%d)", stmt.Info.Min, stmt.Info.Max)}
	}

	// default value for start is min if ascending
	// or max if descending
	if start != nil {
		stmt.Info.Start = *start
	} else if asc {
		stmt.Info.Start = stmt.Info.Min
	} else {
		stmt.Info.Start = stmt.Info.Max
	}

	// check if min < start < max
	if stmt.Info.Start < stmt.Info.Min {
		return nil, &ParseError{Message: fmt.Sprintf("START value (%d) cannot be less than MINVALUE (%d)", stmt.Info.Start, stmt.Info.Min)}
	}
	if stmt.Info.Start > stmt.Info.Max {
		return nil, &ParseError{Message: fmt.Sprintf("START value (%d) cannot be greater than MAXVALUE (%d)", stmt.Info.Start, stmt.Info.Max)}
	}

	// default for cache is 1
	if cache != nil {
		stmt.Info.Cache = uint64(*cache)
	} else {
		stmt.Info.Cache = 1
	}
	return &stmt, err
}

// parseCheckConstraint parses a check constraint.
// it assumes the CHECK token has already been parsed.
func (p *Parser) parseCheckConstraint() (expr.Expr, []document.Path, error) {
	// Parse "("
	err := p.parseTokens(scanner.LPAREN)
	if err != nil {
		return nil, nil, err
	}

	e, err := p.ParseExpr()
	if err != nil {
		return nil, nil, err
	}

	var paths []document.Path
	// extract all the paths from the expression
	expr.Walk(e, func(e expr.Expr) bool {
		switch t := e.(type) {
		case expr.Path:
			pt := document.Path(t)
			// ensure that the path is not already in the list
			found := false
			for _, p := range paths {
				if p.IsEqual(pt) {
					found = true
					break
				}
			}
			if !found {
				paths = append(paths, document.Path(t))
			}
		}

		return true
	})

	// Parse ")"
	err = p.parseTokens(scanner.RPAREN)
	if err != nil {
		return nil, nil, err
	}

	return e, paths, nil
}
