package planner

import (
	"fmt"
	"strings"

	"github.com/genjidb/genji/database"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query/expr"
)

// An AggregationNode is a node that uses aggregate functions to aggregate documents from the stream into one document.
// If the stream contains a Group node, it will generate one document per group.
type AggregationNode struct {
	node

	Aggregators []document.AggregatorBuilder
}

var _ operationNode = (*AggregationNode)(nil)

// NewAggregationNode creates an AggregationNode.
func NewAggregationNode(n Node, aggregators []document.AggregatorBuilder) Node {
	return &AggregationNode{
		node: node{
			op:   Aggregation,
			left: n,
		},
		Aggregators: aggregators,
	}
}

// Bind database resources to this node.
func (n *AggregationNode) Bind(tx *database.Transaction, params []expr.Param) (err error) {
	return
}

func (n *AggregationNode) toStream(st document.Stream) (document.Stream, error) {
	return st.Aggregate(n.Aggregators...), nil
}

func (n *AggregationNode) String() string {
	var b strings.Builder

	for i, ex := range n.Aggregators {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(fmt.Sprintf("%v", ex))
	}

	return fmt.Sprintf("Aggregate(%s)", b.String())
}

// ProjectedGroupAggregatorBuilder references the expression used in the GROUP BY clause
// so that it can be used in the SELECT clause.
type ProjectedGroupAggregatorBuilder struct {
	Expr     expr.Expr
	exprName string
}

// Aggregator implements the document.AggregatorBuilder interface. It creates a projectedGroupAggregator.
func (p *ProjectedGroupAggregatorBuilder) Aggregator(group document.Value) document.Aggregator {
	return &projectedGroupAggregator{
		Name:  p.String(),
		Group: group,
	}
}

func (p *ProjectedGroupAggregatorBuilder) String() string {
	if p.exprName == "" {
		p.exprName = fmt.Sprintf("%v", p.Expr)
	}

	return p.exprName
}

// projectedGroupAggregator implements the document.Aggregator interface.
// It is used to project the GROUP BY expression in the resulting document, once it's aggregated.
type projectedGroupAggregator struct {
	Name  string
	Group document.Value
}

// Add doesn't do anything.
func (p *projectedGroupAggregator) Add(d document.Document) error {
	return nil
}

// Aggregate adds a field to the given buffer with the group value.
func (p *projectedGroupAggregator) Aggregate(fb *document.FieldBuffer) error {
	fb.Add(p.Name, p.Group)
	return nil
}
