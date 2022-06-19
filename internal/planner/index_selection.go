package planner

import (
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/sql/scanner"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/stream/docs"
	"github.com/genjidb/genji/internal/stream/index"
	"github.com/genjidb/genji/internal/stream/table"
)

// SelectIndex attempts to replace a sequential scan by an index scan or a pk scan by
// analyzing the stream for indexable filter nodes.
// It expects the first node of the stream to be a table.Scan.
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
//   table.Scan('foo') | docs.Filter(a > 5) | docs.Filter(b > 10) | docs.Project(*)
// foo_a_idx matches docs.Filter(a > 5) and can be selected.
// Now, with a different index:
//   CREATE INDEX foo_a_b_c_idx ON foo(a, b, c)
// and this query:
//   SELECT * FROM foo WHERE a > 5 AND c > 20
//   table.Scan('foo') | docs.Filter(a > 5) | docs.Filter(c > 20) | docs.Project(*)
// foo_a_b_c_idx matches with the first filter because a is the leftmost path indexed by it.
// The second filter is not selected because it is not the second leftmost path.
// For composite indexes, filter nodes can be selected if they match with one or more indexed path
// consecutively, from left to right.
// Now, let's have a look a this query:
//   SELECT * FROM foo WHERE a = 5 AND b = 10 AND c > 15 AND d > 20
//   table.Scan('foo') | docs.Filter(a = 5) | docs.Filter(b = 10) | docs.Filter(c > 15) | docs.Filter(d > 20) | docs.Project(*)
// foo_a_b_c_idx matches with first three filters because they satisfy several conditions:
// - each of them matches with the first 3 indexed paths, consecutively.
// - the first 2 filters use the equal operator
// A counter-example:
//   SELECT * FROM foo WHERE a = 5 AND b > 10 AND c > 15 AND d > 20
//   table.Scan('foo') | docs.Filter(a = 5) | docs.Filter(b > 10) | docs.Filter(c > 15) | docs.Filter(d > 20) | docs.Project(*)
// foo_a_b_c_idx only matches with the first two filter nodes because while the first node uses the equal
// operator, the second one doesn't, and thus the third node cannot be selected as well.
//
// Candidates and cost
//
// Because a table can have multiple indexes, we need to establish which of these
// indexes should be used to run the query, if not all of them.
// For that we generate a cost for each selected index and return the one with the cheapest cost.
func SelectIndex(sctx *StreamContext) error {
	// Lookup the seq scan node.
	// We will assume that at this point
	// if there is one it has to be the
	// first node of the stream.
	firstNode := sctx.Stream.First()
	if firstNode == nil {
		return nil
	}
	seq, ok := firstNode.(*table.ScanOperator)
	if !ok {
		return nil
	}

	// ensure the table exists
	_, err := sctx.Catalog.GetTableInfo(seq.TableName)
	if err != nil {
		return err
	}

	// ensure the list of filter nodes is not empty
	if len(sctx.Filters) == 0 && len(sctx.TempTreeSorts) == 0 {
		return nil
	}

	is := indexSelector{
		tableScan: seq,
		sctx:      sctx,
	}

	return is.selectIndex()
}

// indexSelector analyses a stream and generates a plan for each of them that
// can benefit from using an index.
// It then compares the cost of each plan and returns the cheapest stream.
type indexSelector struct {
	tableScan *table.ScanOperator
	sctx      *StreamContext
}

func (i *indexSelector) selectIndex() error {
	// generate a list of candidates from all the filter nodes that
	// can benefit from reading from an index or the table pk,
	// plus potentially ORDER BY nodes (1 max)
	nodes := make(indexableNodes, 0, len(i.sctx.Filters)+1)

	// get all contiguous filter nodes that can be indexed
	for _, f := range i.sctx.Filters {
		filter := i.isFilterIndexable(f)
		if filter == nil {
			continue
		}

		nodes = append(nodes, filter)
	}

	// The RemoveUnnecessaryTempSortNodesRule made sure
	// that if there are multiple TempSort nodes, they are
	// using different paths.
	// In this case, we can only associate the first TempSort node
	// with an index, as the second one will be used to sort the
	// results downstream.
	if len(i.sctx.TempTreeSorts) > 0 {
		node := i.isTempTreeSortIndexable(i.sctx.TempTreeSorts[0])
		if node != nil {
			nodes = append(nodes, node)
		}
	}

	// select the cheapest plan
	var selected *candidate
	var cost int

	// start with the primary key of the table
	tb, err := i.sctx.Catalog.GetTableInfo(i.tableScan.TableName)
	if err != nil {
		return err
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
	for _, idxName := range i.sctx.Catalog.ListIndexes(i.tableScan.TableName) {
		idxInfo, err := i.sctx.Catalog.GetIndexInfo(idxName)
		if err != nil {
			return err
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
		return nil
	}

	// remove the filter nodes from the tree
	for _, f := range selected.nodes {
		switch tp := f.node.(type) {
		case *docs.FilterOperator:
			i.sctx.removeFilterNode(tp)
			if f.orderBy != nil {
				i.sctx.removeTempTreeNodeNode(f.orderBy.node.(*docs.TempTreeSortOperator))
			}
		case *docs.TempTreeSortOperator:
			i.sctx.removeTempTreeNodeNode(tp)
		}
	}

	// we replace the seq scan node by the selected root
	s := i.sctx.Stream
	s.Remove(s.First())
	for i := len(selected.replaceRootBy) - 1; i >= 0; i-- {
		if s.Op == nil {
			s.Op = selected.replaceRootBy[i]
			continue
		}
		stream.InsertBefore(s.First(), selected.replaceRootBy[i])
	}
	i.sctx.Stream = s

	return nil
}

func (i *indexSelector) isFilterIndexable(f *docs.FilterOperator) *indexableNode {
	// only operators can associate this node to an index
	op, ok := f.Expr.(expr.Operator)
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

	node := indexableNode{
		node:     f,
		path:     path,
		operator: op.Token(),
		operand:  e,
	}

	return &node
}

func (i *indexSelector) isTempTreeSortIndexable(n *docs.TempTreeSortOperator) *indexableNode {
	// only paths can be associated with an index
	path, ok := n.Expr.(expr.Path)
	if !ok {
		return nil
	}

	return &indexableNode{
		node:     n,
		path:     document.Path(path),
		desc:     n.Desc,
		operator: scanner.ORDER,
	}
}

// for a given index, select all filter nodes that match according to the following rules:
// - from left to right, associate each indexed path to a filter node and stop when there is no
// node available or the node is not compatible
// - for n associated nodes, the n - 1 first must all use the = operator, only the last one
// can be any of =, >, >=, <, <=
// - transform all associated nodes into an index range
// If not all indexed paths have an associated filter node, return whatever has been associated
// A few examples for this index: CREATE INDEX ON foo(a, b, c)
//   fitler(a = 3) | docs.Filter(b = 10) | (c > 20)
//   -> range = {min: [3, 10, 20]}
//   fitler(a = 3) | docs.Filter(b > 10) | (c > 20)
//   -> range = {min: [3], exact: true}
//  docs.Filter(a IN (1, 2))
//   -> ranges = [1], [2]
func (i *indexSelector) associateIndexWithNodes(treeName string, isIndex bool, isUnique bool, paths []document.Path, nodes indexableNodes) *candidate {
	found := make([]*indexableNode, 0, len(paths))
	var desc bool

	var hasIn bool
	var sorter *indexableNode
	for _, p := range paths {
		ns := nodes.getByPath(p)
		if len(ns) == 0 {
			break
		}

		// get the filter node and the TempSort node if any
		var filter *indexableNode
		for i, n := range ns {
			if n.operator == scanner.ORDER && sorter == nil {
				sorter = ns[i]
				desc = sorter.desc
				continue
			}
			if filter == nil {
				filter = ns[i]
			}

			if filter != nil && sorter != nil {
				break
			}
		}

		if filter == nil {
			break
		}

		// if we have both a filter and a TempSort node, we can merge them
		if filter != nil && sorter != nil {
			filter.orderBy = sorter
			sorter = nil
		}

		if filter.operator == scanner.IN {
			hasIn = true
		}

		// in the case there is an IN operator somewhere
		// we only select additional IN or = operators.
		// Otherwise, any operator is accepted
		if !hasIn || (filter.operator == scanner.EQ || filter.operator == scanner.IN) {
			found = append(found, filter)
		}

		// we must stop at the first operator that is not a IN or a =
		if filter.operator != scanner.EQ && filter.operator != scanner.IN {
			break
		}
	}

	if len(found) == 0 && sorter == nil {
		return nil
	}

	// if we only have a TempSort node, we use a scan with no range
	if len(found) == 0 {
		c := candidate{
			nodes:      []*indexableNode{sorter},
			rangesCost: 10_000,
			isIndex:    isIndex,
			isUnique:   isUnique,
		}

		if !isIndex {
			if !desc {
				c.replaceRootBy = []stream.Operator{
					table.Scan(treeName),
				}
			} else {
				c.replaceRootBy = []stream.Operator{
					table.ScanReverse(treeName),
				}
			}
		} else {
			if !desc {
				c.replaceRootBy = []stream.Operator{
					index.Scan(treeName),
				}
			} else {
				c.replaceRootBy = []stream.Operator{
					index.ScanReverse(treeName),
				}
			}
		}

		return &c
	}

	// in case we found an orphan sorter node and we need to assign it to the first filter node
	// for deletion
	if sorter != nil {
		found[0].orderBy = sorter
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
		if !desc {
			c.replaceRootBy = []stream.Operator{
				table.Scan(treeName, ranges...),
			}
		} else {
			c.replaceRootBy = []stream.Operator{
				table.ScanReverse(treeName, ranges...),
			}
		}
	} else {
		if !desc {
			c.replaceRootBy = []stream.Operator{
				index.Scan(treeName, ranges...),
			}
		} else {
			c.replaceRootBy = []stream.Operator{
				index.ScanReverse(treeName, ranges...),
			}
		}
	}

	return &c
}

func (i *indexSelector) buildRangesFromFilterNodes(paths []document.Path, filters []*indexableNode) stream.Ranges {
	// build a 2 dimentional list of all expressions
	// so that: docs.Filter(a IN (10, 11)) | docs.Filter(b = 20) | docs.Filter(c IN (30, 31))
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

func (i *indexSelector) buildRangeFromFilterNodes(filters ...*indexableNode) stream.Range {
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

func (i *indexSelector) buildRangeFromOperator(lastOp scanner.Token, paths []document.Path, operands ...expr.Expr) stream.Range {
	rng := stream.Range{
		Paths: paths,
	}

	el := expr.LiteralExprList(operands)

	switch lastOp {
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
	case scanner.BETWEEN:
		/* example:
		CREATE TABLE test(a int, b int, c int, d int, e int);
		CREATE INDEX on test(a, b, c, d);
		EXPLAIN SELECT * FROM test WHERE a = 1 AND b = 10 AND c = 100 AND d BETWEEN 1000 AND 2000 AND e > 10000;
		{
		    "plan": 'index.Scan("test_a_b_c_d_idx", [{"min": [1, 10, 100, 1000], "max": [1, 10, 100, 2000]}]) | docs.Filter(e > 10000)'
		}
		*/
		rng.Min = make(expr.LiteralExprList, len(el))
		rng.Max = make(expr.LiteralExprList, len(el))
		for i := range el {
			if i == len(el)-1 {
				e := el[i].(expr.LiteralExprList)
				rng.Min[i] = e[0]
				rng.Max[i] = e[1]
				continue
			}

			rng.Min[i] = el[i]
			rng.Max[i] = el[i]
		}
	}

	return rng
}

// an indexableNode is a node that can be used to
// read from an index instead of a table.
// It can be used to filter the results of a query or
// to order the results.
type indexableNode struct {
	// associated stream node (either a DocsFilterNode or a DocsTempTreeSortNote)
	node stream.Operator

	// For filter nodes
	// the expression of the node
	// has been broken into
	// <path> <operator> <operand>
	// Ex:   WHERE a.b[0] > 5 + 5
	// Gives:
	// - path: a.b[0]
	// - operator: scanner.GT
	// - operand: 5 + 5
	// For TempTreeSort nodes
	// the expression of the node
	// has been broken into
	// <path> <direction>
	// Ex:  ORDER BY a.b[0] ASC
	// Gives:
	// - path: a.b[0]
	// - desc: false
	path     document.Path
	operator scanner.Token
	operand  expr.Expr
	desc     bool

	// merged TempTreeSort node to remove
	// from the stream
	orderBy *indexableNode
}

type indexableNodes []*indexableNode

// getByPath returns all indexable nodes for the given path.
// TODO(asdine): add a rule that merges nodes that point to the
// same path.
func (n indexableNodes) getByPath(p document.Path) []*indexableNode {
	var nodes []*indexableNode
	for _, fn := range n {
		if fn.path.IsEqual(p) {
			nodes = append(nodes, fn)
		}
	}

	return nodes
}

type candidate struct {
	// filter operators to remove and replace by either an index.Scan
	// or pkScan operators.
	nodes indexableNodes

	// replace the table.Scan by these nodes
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
	case scanner.EQ, scanner.GT, scanner.GTE, scanner.LT, scanner.LTE, scanner.IN, scanner.BETWEEN:
		return true
	}

	return false
}

func operatorCanUseIndex(op expr.Operator) (bool, document.Path, expr.Expr) {
	lf, leftIsPath := op.LeftHand().(expr.Path)
	rf, rightIsPath := op.RightHand().(expr.Path)

	// Special case for IN operator: only left operand is valid for index usage
	// valid:   a IN [1, 2, 3]
	// invalid: 1 IN a
	// invalid: a IN (b + 1, 2)
	if op.Token() == scanner.IN {
		if leftIsPath && !rightIsPath && !exprContainsPath(op.RightHand()) {
			rh := op.RightHand()
			// The IN operator can use indexes only if the right hand side is an expression list.
			if _, ok := rh.(expr.LiteralExprList); !ok {
				return false, nil, nil
			}
			return true, document.Path(lf), rh
		}

		return false, nil, nil
	}

	// Special case for BETWEEN operator: Given this expression (x BETWEEN a AND b),
	// we can only use the index if the "x" is a path and "a" and "b" don't contain path expressions.
	if op.Token() == scanner.BETWEEN {
		bt := op.(*expr.BetweenOperator)
		x, xIsPath := bt.X.(expr.Path)
		if !xIsPath || exprContainsPath(bt.LeftHand()) || exprContainsPath(bt.RightHand()) {
			return false, nil, nil
		}

		return true, document.Path(x), expr.LiteralExprList{bt.LeftHand(), bt.RightHand()}
	}

	// path OP expr
	if leftIsPath && !rightIsPath && !exprContainsPath(op.RightHand()) {
		return true, document.Path(lf), op.RightHand()
	}

	// expr OP path
	if rightIsPath && !leftIsPath && !exprContainsPath(op.LeftHand()) {
		return true, document.Path(rf), op.LeftHand()
	}

	return false, nil, nil
}

func exprContainsPath(e expr.Expr) bool {
	var hasPath bool

	expr.Walk(e, func(e expr.Expr) bool {
		if _, ok := e.(expr.Path); ok {
			hasPath = true
			return false
		}
		return true
	})

	return hasPath
}
