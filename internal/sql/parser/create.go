package parser

import (
	"fmt"
	"math"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/query/statement"
	"github.com/genjidb/genji/internal/sql/scanner"
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

	// Parse constraints.
	for {
		// we start by checking if it is a table constraint,
		// as it's easier to determine
		ok, err := p.parseTableConstraint(stmt)
		if err != nil {
			return err
		}

		// no table constraint found
		if !ok && parsingTableConstraints {
			tok, pos, lit := p.ScanIgnoreWhitespace()
			return newParseError(scanner.Tokstr(tok, lit), []string{"CONSTRAINT", ")"}, pos)
		}

		// only PRIMARY KEY(path) is currently supported.
		if ok {
			parsingTableConstraints = true
		}

		// if set to false, we are still parsing field definitions
		if !parsingTableConstraints {
			var fc database.FieldConstraint
			err := p.parseFieldDefinition(&fc, &stmt.Info)
			if err != nil {
				return err
			}

			// if the field definition is empty, we ignore it
			if !fc.IsEmpty() {
				err = stmt.Info.FieldConstraints.Add(&fc)
				if err != nil {
					return err
				}
			}
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

	return nil
}

func (p *Parser) parseFieldDefinition(fc *database.FieldConstraint, info *database.TableInfo) error {
	var err error

	fc.Path, err = p.parsePath()
	if err != nil {
		return err
	}

	fc.Type, err = p.parseType()
	if err != nil {
		p.Unscan()
	}

	var addedTc int

LOOP:
	for {
		tok, pos, lit := p.ScanIgnoreWhitespace()
		switch tok {
		case scanner.PRIMARY:
			// Parse "KEY"
			if err := p.parseTokens(scanner.KEY); err != nil {
				return err
			}

			err = info.TableConstraints.AddPrimaryKey(info.TableName, []document.Path{fc.Path})
			if err != nil {
				return err
			}
			addedTc++
		case scanner.NOT:
			// Parse "NULL"
			if err := p.parseTokens(scanner.NULL); err != nil {
				return err
			}

			// if it's already not null we return an error
			if fc.IsNotNull {
				return newParseError(scanner.Tokstr(tok, lit), []string{"CONSTRAINT", ")"}, pos)
			}

			fc.IsNotNull = true
		case scanner.DEFAULT:
			// if it has already a default value we return an error
			if fc.HasDefaultValue() {
				return newParseError(scanner.Tokstr(tok, lit), []string{"CONSTRAINT", ")"}, pos)
			}

			withParentheses, err := p.parseOptional(scanner.LPAREN)
			if err != nil {
				return err
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
				return err
			}

			fc.DefaultValue = expr.Constraint(e)

			if withParentheses {
				_, err = p.parseOptional(scanner.RPAREN)
				if err != nil {
					return err
				}
			}
		case scanner.UNIQUE:
			info.TableConstraints.AddUnique(info.TableName, []document.Path{fc.Path})
			addedTc++
		case scanner.CHECK:
			// Parse "("
			err := p.parseTokens(scanner.LPAREN)
			if err != nil {
				return err
			}

			e, err := p.ParseExpr()
			if err != nil {
				return err
			}

			// Parse ")"
			err = p.parseTokens(scanner.RPAREN)
			if err != nil {
				return err
			}

			info.TableConstraints.AddCheck(info.TableName, expr.Constraint(e))
			addedTc++
		default:
			p.Unscan()
			break LOOP
		}
	}

	// if no constraint was added we return an error. i.e:
	// CREATE TABLE t (a)
	if fc.IsEmpty() && addedTc == 0 {
		tok, pos, lit := p.ScanIgnoreWhitespace()
		return newParseError(scanner.Tokstr(tok, lit), []string{"CONSTRAINT", "TYPE"}, pos)
	}

	return nil
}

func (p *Parser) parseTableConstraint(stmt *statement.CreateTableStmt) (bool, error) {
	var err error

	tok, _, _ := p.ScanIgnoreWhitespace()
	switch tok {
	case scanner.PRIMARY:
		// Parse "KEY ("
		err = p.parseTokens(scanner.KEY)
		if err != nil {
			return false, err
		}

		paths, err := p.parsePathList()
		if err != nil {
			return false, err
		}
		if len(paths) == 0 {
			tok, pos, lit := p.ScanIgnoreWhitespace()
			return false, newParseError(scanner.Tokstr(tok, lit), []string{"PATHS"}, pos)
		}

		if err := stmt.Info.TableConstraints.AddPrimaryKey(stmt.Info.TableName, paths); err != nil {
			return false, err
		}

		return true, nil
	case scanner.UNIQUE:
		paths, err := p.parsePathList()
		if err != nil {
			return false, err
		}
		if len(paths) == 0 {
			tok, pos, lit := p.ScanIgnoreWhitespace()
			return false, newParseError(scanner.Tokstr(tok, lit), []string{"PATHS"}, pos)
		}

		stmt.Info.TableConstraints.AddUnique(stmt.Info.TableName, paths)
		return true, nil
	case scanner.CHECK:
		// Parse "("
		err = p.parseTokens(scanner.LPAREN)
		if err != nil {
			return false, err
		}

		e, err := p.ParseExpr()
		if err != nil {
			return false, err
		}

		// Parse ")"
		err = p.parseTokens(scanner.RPAREN)
		if err != nil {
			return false, err
		}

		stmt.Info.TableConstraints.AddCheck(stmt.Info.TableName, expr.Constraint(e))

		return true, nil
	default:
		p.Unscan()
		return false, nil
	}
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
	stmt.Info.TableName, err = p.parseIdent()
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
