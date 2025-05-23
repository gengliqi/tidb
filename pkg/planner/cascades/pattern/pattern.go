// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pattern

import (
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
)

// Operand is the node of a pattern tree, it represents a logical expression operator.
// Different from logical plan operator which holds the full information about an expression
// operator, Operand only stores the type information.
// An Operand may correspond to a concrete logical plan operator, or it can has special meaning,
// e.g, a placeholder for any logical plan operator.
type Operand int

const (
	// OperandAny is a placeholder for any Operand.
	OperandAny Operand = iota
	// OperandJoin is the operand for LogicalJoin.
	OperandJoin
	// OperandAggregation is the operand for LogicalAggregation.
	OperandAggregation
	// OperandProjection is the operand for LogicalProjection.
	OperandProjection
	// OperandSelection is the operand for LogicalSelection.
	OperandSelection
	// OperandApply is the operand for LogicalApply.
	OperandApply
	// OperandMaxOneRow is the operand for LogicalMaxOneRow.
	OperandMaxOneRow
	// OperandTableDual is the operand for LogicalTableDual.
	OperandTableDual
	// OperandDataSource is the operand for DataSource.
	OperandDataSource
	// OperandUnionScan is the operand for LogicalUnionScan.
	OperandUnionScan
	// OperandUnionAll is the operand for LogicalUnionAll.
	OperandUnionAll
	// OperandSort is the operand for LogicalSort.
	OperandSort
	// OperandTopN is the operand for LogicalTopN.
	OperandTopN
	// OperandLock is the operand for LogicalLock.
	OperandLock
	// OperandLimit is the operand for LogicalLimit.
	OperandLimit
	// OperandTiKVSingleGather is the operand for TiKVSingleGather.
	OperandTiKVSingleGather
	// OperandMemTableScan is the operand for MemTableScan.
	OperandMemTableScan
	// OperandTableScan is the operand for TableScan.
	OperandTableScan
	// OperandIndexScan is the operand for IndexScan.
	OperandIndexScan
	// OperandShow is the operand for Show.
	OperandShow
	// OperandWindow is the operand for window function.
	OperandWindow
	// OperandUnsupported is the operand for unsupported operators.
	OperandUnsupported
)

// String implements fmt.Stringer interface.
func (o Operand) String() string {
	switch o {
	case OperandAny:
		return "OperandAny"
	case OperandJoin:
		return "OperandJoin"
	case OperandAggregation:
		return "OperandAggregation"
	case OperandProjection:
		return "OperandProjection"
	case OperandSelection:
		return "OperandSelection"
	case OperandApply:
		return "OperandApply"
	case OperandMaxOneRow:
		return "OperandMaxOneRow"
	case OperandTableDual:
		return "OperandTableDual"
	case OperandDataSource:
		return "OperandDataSource"
	case OperandUnionScan:
		return "OperandUnionScan"
	case OperandUnionAll:
		return "OperandUnionAll"
	case OperandSort:
		return "OperandSort"
	case OperandTopN:
		return "OperandTopN"
	case OperandLock:
		return "OperandLock"
	case OperandLimit:
		return "OperandLimit"
	case OperandTiKVSingleGather:
		return "OperandTiKVSingleGather"
	case OperandMemTableScan:
		return "OperandMemTableScan"
	case OperandTableScan:
		return "OperandTableScan"
	case OperandIndexScan:
		return "OperandIndexScan"
	case OperandShow:
		return "OperandShow"
	case OperandWindow:
		return "OperandWindow"
	default:
		return "OperandUnsupported"
	}
}

// GetOperand maps logical plan operator to Operand.
func GetOperand(p base.LogicalPlan) Operand {
	switch p.(type) {
	case *logicalop.LogicalApply:
		return OperandApply
	case *logicalop.LogicalJoin:
		return OperandJoin
	case *logicalop.LogicalAggregation:
		return OperandAggregation
	case *logicalop.LogicalProjection:
		return OperandProjection
	case *logicalop.LogicalSelection:
		return OperandSelection
	case *logicalop.LogicalMaxOneRow:
		return OperandMaxOneRow
	case *logicalop.LogicalTableDual:
		return OperandTableDual
	case *logicalop.DataSource:
		return OperandDataSource
	case *logicalop.LogicalUnionScan:
		return OperandUnionScan
	case *logicalop.LogicalUnionAll:
		return OperandUnionAll
	case *logicalop.LogicalSort:
		return OperandSort
	case *logicalop.LogicalTopN:
		return OperandTopN
	case *logicalop.LogicalLock:
		return OperandLock
	case *logicalop.LogicalLimit:
		return OperandLimit
	case *logicalop.TiKVSingleGather:
		return OperandTiKVSingleGather
	case *logicalop.LogicalTableScan:
		return OperandTableScan
	case *logicalop.LogicalMemTable:
		return OperandMemTableScan
	case *logicalop.LogicalIndexScan:
		return OperandIndexScan
	case *logicalop.LogicalShow:
		return OperandShow
	case *logicalop.LogicalWindow:
		return OperandWindow
	default:
		return OperandUnsupported
	}
}

// Match checks if current Operand matches specified one.
func (o Operand) Match(t Operand) bool {
	if o == OperandAny || t == OperandAny {
		return true
	}
	if o == t {
		return true
	}
	return false
}

// Pattern defines the match pattern for a rule. It's a tree-like structure
// which is a piece of a logical expression. Each node in the Pattern tree is
// defined by an Operand and EngineType pair.
type Pattern struct {
	Operand
	EngineTypeSet
	Children []*Pattern
}

// Match checks whether the EngineTypeSet contains the given EngineType
// and whether the two Operands match.
func (p *Pattern) Match(o Operand, e EngineType) bool {
	return p.EngineTypeSet.Contains(e) && p.Operand.Match(o)
}

// MatchOperandAny checks whether the pattern's Operand is OperandAny
// and the EngineTypeSet contains the given EngineType.
func (p *Pattern) MatchOperandAny(e EngineType) bool {
	return p.EngineTypeSet.Contains(e) && p.Operand == OperandAny
}

// NewPattern creates a pattern node according to the Operand and EngineType.
func NewPattern(operand Operand, engineTypeSet EngineTypeSet) *Pattern {
	return &Pattern{Operand: operand, EngineTypeSet: engineTypeSet}
}

// SetChildren sets the Children information for a pattern node.
func (p *Pattern) SetChildren(children ...*Pattern) {
	p.Children = children
}

// BuildPattern builds a Pattern from Operand, EngineType and child Patterns.
// Used in GetPattern() of Transformation interface to generate a Pattern.
func BuildPattern(operand Operand, engineTypeSet EngineTypeSet, children ...*Pattern) *Pattern {
	p := &Pattern{Operand: operand, EngineTypeSet: engineTypeSet}
	p.Children = children
	return p
}
