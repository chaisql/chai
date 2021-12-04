package planner

import (
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/sql/scanner"
	"github.com/genjidb/genji/internal/stream"
)

// SelectIndex attempts to replace a sequential scan by an index scan or a pk scan by
// analyzing the stream for indexable filter nodes.
// It expects the first node of the stream to be a seqScan.
//
// Compatibility of filter nodes.
//
// For a filter node to be selected if must be of the following form:
//   <path> <compatible operator> <expression>
// or
//   <expression> <compatible operator> <path>
// path: path of a document
// compatible operator: one of =, >, >=, <, <=, IN
// expression: any expression
//
// Index compatibility.
//
// Once we have a list of all compatible filter nodes, we try to associate
// indexes with them.
// Given the following index:
//   CREATE INDEX foo_a_idx ON foo (a)
// and this query:
//   SELECT * FROM foo WHERE a > 5 AND b > 10
//   seqScan('foo') | filter(a > 5) | filter(b > 10) | project(*)
// foo_a_idx matches filter(a > 5) and can be selected.
// Now, with a different index:
//   CREATE INDEX foo_a_b_c_idx ON foo(a, b, c)
// and this query:
//   SELECT * FROM foo WHERE a > 5 AND c > 20
//   seqScan('foo') | filter(a > 5) | filter(c > 20) | project(*)
// foo_a_b_c_idx matches with the first filter because a is the leftmost path indexed by it.
// The second filter is not selected because it is not the second leftmost path.
// For composite indexes, filter nodes can be selected if they match with one or more indexed path
// consecutively, from left to right.
// Now, let's have a look a this query:
//   SELECT * FROM foo WHERE a = 5 AND b = 10 AND c > 15 AND d > 20
//   seqScan('foo') | filter(a = 5) | filter(b = 10) | filter(c > 15) | filter(d > 20) | project(*)
// foo_a_b_c_idx matches with first three filters because they satisfy several conditions:
// - each of them matches with the first 3 indexed paths, consecutively.
// - the first 2 filters use the equal operator
// A counter-example:
//   SELECT * FROM foo WHERE a = 5 AND b > 10 AND c > 15 AND d > 20
//   seqScan('foo') | filter(a = 5) | filter(b > 10) | filter(c > 15) | filter(d > 20) | project(*)
// foo_a_b_c_idx only matches with the first two filter nodes because while the first node uses the equal
// operator, the second one doesn't, and thus the third node cannot be selected as well.
//
// Candidates and cost
//
// Because a table can have multiple indexes, we need to establish which of these
// indexes should be used to run the query, if not all of them.
// For that we generate a cost for each selected index and return the one with the cheapest cost.
func SelectIndex(s *stream.Stream, catalog *database.Catalog) (*stream.Stream, error) {
	// first we lookup for the seq scan node.
	// Here we will assume that at this point
	// if there is one it has to be the
	// first node of the stream.
	firstNode := s.First()
	if firstNode == nil {
		return s, nil
	}
	seq, ok := firstNode.(*stream.SeqScanOperator)
	if !ok {
		return s, nil
	}

	// ensure the table exists
	_, err := catalog.Cache.Get(database.RelationTableType, seq.TableName)
	if err != nil {
		return nil, err
	}

	is := indexSelector{
		seqScan: seq,
		catalog: catalog,
	}

	return is.SelectIndex(s)
}

// indexSelector analyses a stream and generates a plan for each of them that
// can benefit from using an index.
// It then compares the cost of each plan and returns the cheapest stream.
type indexSelector struct {
	seqScan *stream.SeqScanOperator
	catalog *database.Catalog
}

func (i *indexSelector) SelectIndex(s *stream.Stream) (*stream.Stream, error) {
	// get the list of all filter nodes
	var filterNodes []*stream.FilterOperator
	for op := s.Op; op != nil; op = op.GetPrev() {
		if f, ok := op.(*stream.FilterOperator); ok {
			filterNodes = append(filterNodes, f)
		}
	}

	// if there are no filter, return the stream untouched
	if len(filterNodes) == 0 {
		return s, nil
	}

	return i.selectIndex(s, filterNodes)
}

func (i *indexSelector) selectIndex(s *stream.Stream, filters []*stream.FilterOperator) (*stream.Stream, error) {
	// generate a list of candidates from all the filter nodes that
	// can benefit from reading from an index or the table pk
	nodes := make(filterNodes, 0, len(filters))
	for _, f := range filters {
		filter := i.isFilterIndexable(f)
		if filter == nil {
			continue
		}

		nodes = append(nodes, filter)
	}

	// select the cheapest plan
	var selected *candidate
	var cost int

	// start with the primary key of the table
	tb, err := i.catalog.GetTableInfo(i.seqScan.TableName)
	if err != nil {
		return nil, err
	}
	pk := tb.GetPrimaryKey()
	if pk != nil {
		selected = i.associateIndexWithNodes(tb.TableName, false, false, pk.Paths, nodes)
		if selected != nil {
			cost = selected.Cost()
		}
	}

	// get all the indexes for this table and associate them
	// with compatible candidates
	for _, idxName := range i.catalog.ListIndexes(i.seqScan.TableName) {
		idxInfo, err := i.catalog.GetIndexInfo(idxName)
		if err != nil {
			return nil, err
		}

		candidate := i.associateIndexWithNodes(idxInfo.IndexName, true, idxInfo.Unique, idxInfo.Paths, nodes)

		if candidate == nil {
			continue
		}

		if selected == nil {
			selected = candidate
			cost = selected.Cost()
			continue
		}

		c := candidate.Cost()

		if len(selected.nodes) < len(candidate.nodes) || (len(selected.nodes) == len(candidate.nodes) && c < cost) {
			cost = c
			selected = candidate
		}
	}

	if selected == nil {
		return s, nil
	}

	// remove the filter nodes from the tree
	for _, f := range selected.nodes {
		s.Remove(f.node)
	}

	// we replace the seq scan node by the selected root
	s.Remove(s.First())
	for i := len(selected.replaceRootBy) - 1; i >= 0; i-- {
		if s.Op == nil {
			s.Op = selected.replaceRootBy[i]
			continue
		}
		stream.InsertBefore(s.First(), selected.replaceRootBy[i])
	}

	return s, nil
}

func (i *indexSelector) isFilterIndexable(f *stream.FilterOperator) *filterNode {
	// only operators can associate this node to an index
	op, ok := f.E.(expr.Operator)
	if !ok {
		return nil
	}

	// ensure the operator is compatible
	if !operatorIsIndexCompatible(op) {
		return nil
	}

	// determine if the operator could benefit from an index
	ok, path, e := operatorCanUseIndex(op)
	if !ok {
		return nil
	}

	node := filterNode{
		node:     f,
		path:     path,
		operator: op.Token(),
		operand:  e,
	}

	return &node
}

// for a given index, select all filter nodes that match according to the following rules:
// - from left to right, associate each indexed path to a filter node and stop when there is no
// node available or the node is not compatible
// - for n associated nodes, the n - 1 first must all use the = operator, only the last one
// can be any of =, >, >=, <, <=
// - transform all associated nodes into an index range
// If not all indexed paths have an associated filter node, return whatever has been associated
// A few examples for this index: CREATE INDEX ON foo(a, b, c)
//   fitler(a = 3) | filter(b = 10) | (c > 20)
//   -> range = {min: [3, 10, 20]}
//   fitler(a = 3) | filter(b > 10) | (c > 20)
//   -> range = {min: [3], exact: true}
//  filter(a IN (1, 2))
//   -> ranges = [1], [2]
func (i *indexSelector) associateIndexWithNodes(treeName string, isIndex bool, isUnique bool, paths []document.Path, nodes filterNodes) *candidate {
	found := make([]*filterNode, 0, len(paths))

	var hasIn bool
	for _, p := range paths {
		n := nodes.getByPath(p)
		if n == nil {
			break
		}

		if n.operator == scanner.IN {
			hasIn = true
		}

		// in the case there is an IN operator somewhere
		// we only select additional IN or = operators.
		// Otherwise, any operator is accepted
		if !hasIn || (n.operator == scanner.EQ || n.operator == scanner.IN) {
			found = append(found, n)
		}

		// we must stop at the first operator that is not a IN or a =
		if n.operator != scanner.EQ && n.operator != scanner.IN {
			break
		}
	}

	if len(found) == 0 {
		return nil
	}

	// in case there is an IN operator in the list, we need to generate multiple ranges.
	// If not, we only need one range.
	var ranges stream.Ranges

	if !hasIn {
		ranges = stream.Ranges{i.buildRangeFromFilterNodes(found...)}
	} else {
		ranges = i.buildRangesFromFilterNodes(paths, found)
	}

	c := candidate{
		nodes:      found,
		rangesCost: ranges.Cost(),
		isIndex:    isIndex,
		isUnique:   isUnique,
	}

	if !isIndex {
		c.replaceRootBy = []stream.Operator{
			stream.PkScan(treeName, ranges...),
		}
	} else {
		c.replaceRootBy = []stream.Operator{
			stream.IndexScan(treeName, ranges...),
		}
	}

	return &c
}

func (i *indexSelector) buildRangesFromFilterNodes(paths []document.Path, filters []*filterNode) stream.Ranges {
	// build a 2 dimentional list of all expressions
	// so that: filter(a IN (10, 11)) | filter(b = 20) | filter(c IN (30, 31))
	// becomes:
	// [10, 11]
	// [20]
	// [30, 31]

	l := make([][]expr.Expr, 0, len(filters))

	for _, f := range filters {
		var row []expr.Expr
		if f.operator != scanner.IN {
			row = []expr.Expr{f.operand}
		} else {
			row = f.operand.(expr.LiteralExprList)
		}

		l = append(l, row)
	}

	// generate a list of combinaison between each row of the list
	// Example for the list above:
	// 10, 20, 30
	// 10, 20, 31
	// 11, 20, 30
	// 11, 20, 31

	var ranges stream.Ranges

	i.walkExpr(l, func(row []expr.Expr) {
		ranges = append(ranges, i.buildRangeFromOperator(scanner.EQ, paths[:len(row)], row...))
	})

	return ranges
}

func (i *indexSelector) walkExpr(l [][]expr.Expr, fn func(row []expr.Expr)) {
	curLine := l[0]

	if len(l) == 0 {
		return
	}

	if len(l) == 1 {
		for _, e := range curLine {
			fn([]expr.Expr{e})
		}

		return
	}

	for _, e := range curLine {
		i.walkExpr(l[1:], func(row []expr.Expr) {
			fn(append([]expr.Expr{e}, row...))
		})
	}
}

func (i *indexSelector) buildRangeFromFilterNodes(filters ...*filterNode) stream.Range {
	// first, generate a list of paths and a list of expressions
	paths := make([]document.Path, 0, len(filters))
	el := make(expr.LiteralExprList, 0, len(filters))
	for i := range filters {
		paths = append(paths, filters[i].path)
		el = append(el, filters[i].operand)
	}

	// use last filter node to determine the direction of the range
	filter := filters[len(filters)-1]

	return i.buildRangeFromOperator(filter.operator, paths, el...)
}

func (i *indexSelector) buildRangeFromOperator(op scanner.Token, paths []document.Path, operands ...expr.Expr) stream.Range {
	rng := stream.Range{
		Paths: paths,
	}

	el := expr.LiteralExprList(operands)

	switch op {
	case scanner.EQ, scanner.IN:
		rng.Exact = true
		rng.Min = el
	case scanner.GT:
		rng.Exclusive = true
		rng.Min = el
	case scanner.GTE:
		rng.Min = el
	case scanner.LT:
		rng.Exclusive = true
		rng.Max = el
	case scanner.LTE:
		rng.Max = el
	}

	return rng
}

type filterNode struct {
	// associated stream node
	node stream.Operator

	// the expression of the node
	// has been broken into
	// <path> <operator> <operand>
	// Ex:    a.b[0] > 5 + 5
	// Gives:
	// - path: a.b[0]
	// - operator: scanner.GT
	// - operand: 5 + 5
	path     document.Path
	operator scanner.Token
	operand  expr.Expr
}

type filterNodes []*filterNode

// getByPath returns the first filter for the given path.
// TODO(asdine): add a rule that merges filter nodes that point to the
// same path.
func (f filterNodes) getByPath(p document.Path) *filterNode {
	for _, fn := range f {
		if fn.path.IsEqual(p) {
			return fn
		}
	}

	return nil
}

type candidate struct {
	// filter operators to remove and replace by either an indexScan
	// or pkScan operators.
	nodes filterNodes

	// replace the seqScan by these nodes
	replaceRootBy []stream.Operator

	// cost of the associated ranges
	rangesCost int

	// is this candidate reading from an index.
	// if false, we are reading from the table
	// primary key.
	isIndex bool
	// if it's an index, does it have a unique constraint
	isUnique bool
}

func (c *candidate) Cost() int {
	// we start with the cost of ranges
	cost := c.rangesCost

	if c.isIndex {
		cost += 20
	}
	if c.isUnique {
		cost -= 10
	}

	cost -= len(c.nodes)

	return cost
}

// operatorIsIndexCompatible returns whether the operator can be used to read from an index.
func operatorIsIndexCompatible(op expr.Operator) bool {
	switch op.Token() {
	case scanner.EQ, scanner.GT, scanner.GTE, scanner.LT, scanner.LTE, scanner.IN:
		return true
	}

	return false
}
