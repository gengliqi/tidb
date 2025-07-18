// Copyright 2015 PingCAP, Inc.
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

package core

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/sessiontxn/staleread"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/temptable"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/domainutil"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"go.uber.org/zap"
)

// PreprocessOpt presents optional parameters to `Preprocess` method.
type PreprocessOpt func(*preprocessor)

// InPrepare is a PreprocessOpt that indicates preprocess is executing under prepare statement.
func InPrepare(p *preprocessor) {
	p.flag |= inPrepare
}

// InTxnRetry is a PreprocessOpt that indicates preprocess is executing under transaction retry.
func InTxnRetry(p *preprocessor) {
	p.flag |= inTxnRetry
}

// InitTxnContextProvider is a PreprocessOpt that indicates preprocess should init transaction's context
func InitTxnContextProvider(p *preprocessor) {
	p.flag |= initTxnContextProvider
}

// WithPreprocessorReturn returns a PreprocessOpt to initialize the PreprocessorReturn.
func WithPreprocessorReturn(ret *PreprocessorReturn) PreprocessOpt {
	return func(p *preprocessor) {
		p.PreprocessorReturn = ret
	}
}

// TryAddExtraLimit trys to add an extra limit for SELECT or UNION statement when sql_select_limit is set.
func TryAddExtraLimit(ctx sessionctx.Context, node ast.StmtNode) ast.StmtNode {
	if ctx.GetSessionVars().SelectLimit == math.MaxUint64 || ctx.GetSessionVars().InRestrictedSQL {
		return node
	}
	if explain, ok := node.(*ast.ExplainStmt); ok {
		explain.Stmt = TryAddExtraLimit(ctx, explain.Stmt)
		return explain
	} else if sel, ok := node.(*ast.SelectStmt); ok {
		if sel.Limit != nil || sel.SelectIntoOpt != nil {
			return node
		}
		newSel := *sel
		newSel.Limit = &ast.Limit{
			Count: ast.NewValueExpr(ctx.GetSessionVars().SelectLimit, "", ""),
		}
		return &newSel
	} else if show, ok := node.(*ast.ShowStmt); ok {
		// Only when Limit is nil, for Show stmt Limit should always nil when be here,
		// and the show STMT's behavior should consist with MySQL does.
		if show.Limit != nil || !show.NeedLimitRSRow() {
			return node
		}
		newShow := *show
		newShow.Limit = &ast.Limit{
			Count: ast.NewValueExpr(ctx.GetSessionVars().SelectLimit, "", ""),
		}
		return &newShow
	} else if setOprStmt, ok := node.(*ast.SetOprStmt); ok {
		if setOprStmt.Limit != nil {
			return node
		}
		newSetOpr := *setOprStmt
		newSetOpr.Limit = &ast.Limit{
			Count: ast.NewValueExpr(ctx.GetSessionVars().SelectLimit, "", ""),
		}
		return &newSetOpr
	}
	return node
}

// Preprocess resolves table names of the node, and checks some statements' validation.
// preprocessReturn used to extract the infoschema for the tableName and the timestamp from the asof clause.
func Preprocess(ctx context.Context, sctx sessionctx.Context, node *resolve.NodeW, preprocessOpt ...PreprocessOpt) error {
	defer tracing.StartRegion(ctx, "planner.Preprocess").End()
	v := preprocessor{
		ctx:                ctx,
		sctx:               sctx,
		tableAliasInJoin:   make([]map[string]any, 0),
		preprocessWith:     &preprocessWith{cteCanUsed: make([]string, 0), cteBeforeOffset: make([]int, 0)},
		staleReadProcessor: staleread.NewStaleReadProcessor(ctx, sctx),
		varsMutable:        make(map[string]struct{}),
		varsReadonly:       make(map[string]struct{}),
		resolveCtx:         node.GetResolveContext(),
	}
	for _, optFn := range preprocessOpt {
		optFn(&v)
	}
	// PreprocessorReturn must be non-nil before preprocessing
	if v.PreprocessorReturn == nil {
		v.PreprocessorReturn = &PreprocessorReturn{}
	}
	node.Node.Accept(&v)
	// InfoSchema must be non-nil after preprocessing
	v.ensureInfoSchema()
	sctx.GetPlanCtx().SetReadonlyUserVarMap(v.varsReadonly)
	if len(v.varsReadonly) > 0 {
		sctx.GetSessionVars().StmtCtx.SetSkipPlanCache("read-only variables are used")
	}
	return errors.Trace(v.err)
}

type preprocessorFlag uint64

const (
	// inPrepare is set when visiting in prepare statement.
	inPrepare preprocessorFlag = 1 << iota
	// inTxnRetry is set when visiting in transaction retry.
	inTxnRetry
	// inCreateOrDropTable is set when visiting create/drop table/view/sequence,
	// rename table, alter table add foreign key, alter table in prepare stmt, and BR restore.
	// TODO need a better name to clarify it's meaning
	inCreateOrDropTable
	// parentIsJoin is set when visiting node's parent is join.
	parentIsJoin
	// inRepairTable is set when visiting a repair table statement.
	inRepairTable
	// inSequenceFunction is set when visiting a sequence function.
	// This flag indicates the tableName in these function should be checked as sequence object.
	inSequenceFunction
	// initTxnContextProvider is set when we should init txn context in preprocess
	initTxnContextProvider
	// inImportInto is set when visiting an import into statement.
	inImportInto
	// inAnalyze is set when visiting an analyze statement.
	inAnalyze
)

// Make linter happy.
var _ = PreprocessorReturn{}.initedLastSnapshotTS

// PreprocessorReturn is used to retain information obtained in the preprocessor.
type PreprocessorReturn struct {
	initedLastSnapshotTS bool
	IsStaleness          bool
	SnapshotTSEvaluator  func(context.Context, sessionctx.Context) (uint64, error)
	// LastSnapshotTS is the last evaluated snapshotTS if any
	// otherwise it defaults to zero
	LastSnapshotTS uint64
	InfoSchema     infoschema.InfoSchema
}

// preprocessWith is used to record info from WITH statements like CTE name.
type preprocessWith struct {
	cteCanUsed      []string
	cteBeforeOffset []int
	// A stack is implemented using a two-dimensional array.
	// Each layer stores the cteList of the current query block.
	// For example:
	// Query: with cte1 as (with cte2 as (select * from t) select * from cte2) select * from cte1;
	// Query Block1: select * from t
	// cteStack: [[cte1],[cte2]] (when withClause is null, the cteStack will not be appended)
	// Query Block2: with cte2 as (xxx) select * from cte2
	// cteStack: [[cte1],[cte2]]
	// Query Block3: with cte1 as (xxx) select * from cte1;
	// cteStack: [[cte1]]
	// ** Only the cteStack of SelectStmt and SetOprStmt will be set. **
	cteStack [][]*ast.CommonTableExpression
}

func (pw *preprocessWith) UpdateCTEConsumerCount(tableName string) {
	// must search from the back to the front (from the inner layer to the outer layer)
	// For example:
	// Query: with cte1 as (with cte1 as (select * from t) select * from cte1) select * from cte1;
	// cteStack: [[cte1: outer, consumerCount=1], [cte1: inner, consumerCount=1]]
	for i := len(pw.cteStack) - 1; i >= 0; i-- {
		for _, cte := range pw.cteStack[i] {
			if cte.Name.L == tableName {
				cte.ConsumerCount++
				return
			}
		}
	}
}

// preprocessor is an ast.Visitor that preprocess
// ast Nodes parsed from parser.
type preprocessor struct {
	ctx    context.Context
	sctx   sessionctx.Context
	flag   preprocessorFlag
	stmtTp byte
	showTp ast.ShowStmtType

	// tableAliasInJoin is a stack that keeps the table alias names for joins.
	// len(tableAliasInJoin) may bigger than 1 because the left/right child of join may be subquery that contains `JOIN`
	tableAliasInJoin []map[string]any
	preprocessWith   *preprocessWith

	staleReadProcessor staleread.Processor

	varsMutable  map[string]struct{}
	varsReadonly map[string]struct{}

	// values that may be returned
	*PreprocessorReturn
	err error

	resolveCtx *resolve.Context
}

func (p *preprocessor) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch node := in.(type) {
	case *ast.AdminStmt:
		p.checkAdminCheckTableGrammar(node)
	case *ast.DeleteStmt:
		p.stmtTp = TypeDelete
	case *ast.SelectStmt:
		p.stmtTp = TypeSelect
		if node.With != nil {
			p.preprocessWith.cteStack = append(p.preprocessWith.cteStack, node.With.CTEs)
		}
		p.checkSelectNoopFuncs(node)
	case *ast.SetOprStmt:
		if node.With != nil {
			p.preprocessWith.cteStack = append(p.preprocessWith.cteStack, node.With.CTEs)
		}
	case *ast.UpdateStmt:
		p.stmtTp = TypeUpdate
	case *ast.InsertStmt:
		p.stmtTp = TypeInsert
		// handle the insert table name imminently
		// insert into t with t ..., the insert can not see t here. We should hand it before the CTE statement
		p.handleTableName(node.Table.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName))
	case *ast.ExecuteStmt:
		p.stmtTp = TypeExecute
		p.resolveExecuteStmt(node)
	case *ast.CreateTableStmt:
		p.stmtTp = TypeCreate
		p.flag |= inCreateOrDropTable
		p.resolveCreateTableStmt(node)
		p.checkCreateTableGrammar(node)
	case *ast.CreateViewStmt:
		p.stmtTp = TypeCreate
		p.flag |= inCreateOrDropTable
		p.checkCreateViewGrammar(node)
		p.checkCreateViewWithSelectGrammar(node)
	case *ast.DropTableStmt:
		p.flag |= inCreateOrDropTable
		p.stmtTp = TypeDrop
		p.checkDropTableGrammar(node)
	case *ast.RenameTableStmt:
		p.stmtTp = TypeRename
		p.flag |= inCreateOrDropTable
		p.checkRenameTableGrammar(node)
	case *ast.CreateIndexStmt:
		// Used in CREATE INDEX ...
		p.stmtTp = TypeCreate
		if p.flag&inPrepare != 0 {
			p.flag |= inCreateOrDropTable
		}
		p.checkCreateIndexGrammar(node)
	case *ast.AlterTableStmt:
		p.stmtTp = TypeAlter
		if p.flag&inPrepare != 0 {
			p.flag |= inCreateOrDropTable
		}
		p.resolveAlterTableStmt(node)
		p.checkAlterTableGrammar(node)
	case *ast.CreateDatabaseStmt:
		p.stmtTp = TypeCreate
		p.checkCreateDatabaseGrammar(node)
	case *ast.AlterDatabaseStmt:
		p.stmtTp = TypeAlter
		p.checkAlterDatabaseGrammar(node)
	case *ast.DropDatabaseStmt:
		p.stmtTp = TypeDrop
		p.checkDropDatabaseGrammar(node)
	case *ast.ShowStmt:
		p.stmtTp = TypeShow
		p.showTp = node.Tp
		p.resolveShowStmt(node)
	case *ast.SetOprSelectList:
		if node.With != nil {
			p.preprocessWith.cteStack = append(p.preprocessWith.cteStack, node.With.CTEs)
		}
		p.checkSetOprSelectList(node)
	case *ast.DeleteTableList:
		p.stmtTp = TypeDelete
		return in, true
	case *ast.Join:
		p.checkNonUniqTableAlias(node)
	case *ast.CreateBindingStmt:
		p.stmtTp = TypeCreate
		if node.OriginNode != nil {
			// if node.PlanDigests is not empty, this binding will be created from history, the node.OriginNode and node.HintedNode should be nil
			EraseLastSemicolon(node.OriginNode)
			EraseLastSemicolon(node.HintedNode)
			p.checkBindGrammar(node.OriginNode, node.HintedNode, p.sctx.GetSessionVars().CurrentDB)
		}
		return in, true
	case *ast.DropBindingStmt:
		p.stmtTp = TypeDrop
		if node.OriginNode != nil {
			EraseLastSemicolon(node.OriginNode)
			if node.HintedNode != nil {
				EraseLastSemicolon(node.HintedNode)
				p.checkBindGrammar(node.OriginNode, node.HintedNode, p.sctx.GetSessionVars().CurrentDB)
			}
		}
		return in, true
	case *ast.RecoverTableStmt:
		// The specified table in recover table statement maybe already been dropped.
		// So skip check table name here, otherwise, recover table [table_name] syntax will return
		// table not exists error. But recover table statement is use to recover the dropped table. So skip children here.
		return in, true
	case *ast.FlashBackTableStmt:
		if len(node.NewName) > 0 {
			p.checkFlashbackTableGrammar(node)
		}
		return in, true
	case *ast.FlashBackDatabaseStmt:
		if len(node.NewName) > 0 {
			p.checkFlashbackDatabaseGrammar(node)
		}
		return in, true
	case *ast.RepairTableStmt:
		p.stmtTp = TypeRepair
		// The RepairTable should consist of the logic for creating tables and renaming tables.
		p.flag |= inRepairTable
		p.checkRepairTableGrammar(node)
	case *ast.ImportIntoStmt:
		p.stmtTp = TypeImportInto
		p.flag |= inImportInto
	case *ast.CreateSequenceStmt:
		p.stmtTp = TypeCreate
		p.flag |= inCreateOrDropTable
		p.resolveCreateSequenceStmt(node)
	case *ast.DropSequenceStmt:
		p.stmtTp = TypeDrop
		p.flag |= inCreateOrDropTable
		p.checkDropSequenceGrammar(node)
	case *ast.FuncCastExpr:
		p.checkFuncCastExpr(node)
	case *ast.FuncCallExpr:
		if node.FnName.L == ast.NextVal || node.FnName.L == ast.LastVal || node.FnName.L == ast.SetVal {
			p.flag |= inSequenceFunction
		}
		// not support procedure right now.
		if node.Schema.L != "" {
			p.err = expression.ErrFunctionNotExists.GenWithStackByArgs("FUNCTION", node.Schema.L+"."+node.FnName.L)
		}
	case *ast.BRIEStmt:
		if node.Kind == ast.BRIEKindRestore {
			p.flag |= inCreateOrDropTable
		}
	case *ast.TableSource:
		isModeOracle := p.sctx.GetSessionVars().SQLMode&mysql.ModeOracle != 0
		_, isSelectStmt := node.Source.(*ast.SelectStmt)
		_, isSetOprStmt := node.Source.(*ast.SetOprStmt)
		if (isSelectStmt || isSetOprStmt) && !isModeOracle && len(node.AsName.L) == 0 {
			p.err = dbterror.ErrDerivedMustHaveAlias.GenWithStackByArgs()
		}
		if v, ok := node.Source.(*ast.TableName); ok && v.TableSample != nil {
			switch v.TableSample.SampleMethod {
			case ast.SampleMethodTypeTiDBRegion:
			default:
				p.err = expression.ErrInvalidTableSample.GenWithStackByArgs("Only supports REGIONS sampling method")
			}
		}
	case *ast.GroupByClause:
		p.checkGroupBy(node)
	case *ast.CommonTableExpression, *ast.SubqueryExpr:
		with := p.preprocessWith
		beforeOffset := len(with.cteCanUsed)
		with.cteBeforeOffset = append(with.cteBeforeOffset, beforeOffset)
		if cteNode, exist := node.(*ast.CommonTableExpression); exist && cteNode.IsRecursive {
			with.cteCanUsed = append(with.cteCanUsed, cteNode.Name.L)
		}
	case *ast.BeginStmt:
		// If the begin statement was like following:
		// start transaction read only as of timestamp ....
		// then we need set StmtCtx.IsStaleness as true in order to avoid take tso in PrepareTSFuture.
		if node.AsOf != nil {
			p.sctx.GetSessionVars().StmtCtx.IsStaleness = true
			p.IsStaleness = true
		} else if p.sctx.GetSessionVars().TxnReadTS.PeakTxnReadTS() > 0 {
			// If the begin statement was like following:
			// set transaction read only as of timestamp ...
			// begin
			// then we need set StmtCtx.IsStaleness as true in order to avoid take tso in PrepareTSFuture.
			p.sctx.GetSessionVars().StmtCtx.IsStaleness = true
			p.IsStaleness = true
		}
	case *ast.AnalyzeTableStmt:
		p.flag |= inAnalyze
	case *ast.VariableExpr:
		if node.Value != nil {
			p.varsMutable[node.Name] = struct{}{}
			delete(p.varsReadonly, node.Name)
		} else if p.stmtTp == TypeSelect {
			// Only check the variable in select statement.
			_, ok := p.varsMutable[node.Name]
			if !ok {
				p.varsReadonly[node.Name] = struct{}{}
			}
		}
	case *ast.Constraint:
		// Used in ALTER TABLE or CREATE TABLE
		p.checkConstraintGrammar(node)
	default:
		p.flag &= ^parentIsJoin
	}
	return in, p.err != nil
}

// EraseLastSemicolon removes last semicolon of sql.
func EraseLastSemicolon(stmt ast.StmtNode) {
	sql := stmt.Text()
	if len(sql) > 0 && sql[len(sql)-1] == ';' {
		stmt.SetText(nil, sql[:len(sql)-1])
	}
}

// EraseLastSemicolonInSQL removes last semicolon of the SQL.
func EraseLastSemicolonInSQL(sql string) string {
	if len(sql) > 0 && sql[len(sql)-1] == ';' {
		return sql[:len(sql)-1]
	}
	return sql
}

const (
	// TypeInvalid for unexpected types.
	TypeInvalid byte = iota
	// TypeSelect for SelectStmt.
	TypeSelect
	// TypeSetOpr for SetOprStmt.
	TypeSetOpr
	// TypeDelete for DeleteStmt.
	TypeDelete
	// TypeUpdate for UpdateStmt.
	TypeUpdate
	// TypeInsert for InsertStmt.
	TypeInsert
	// TypeDrop for DropStmt
	TypeDrop
	// TypeCreate for CreateStmt
	TypeCreate
	// TypeAlter for AlterStmt
	TypeAlter
	// TypeRename for RenameStmt
	TypeRename
	// TypeRepair for RepairStmt
	TypeRepair
	// TypeShow for ShowStmt
	TypeShow
	// TypeExecute for ExecuteStmt
	TypeExecute
	// TypeImportInto for ImportIntoStmt
	TypeImportInto
)

func bindableStmtType(node ast.StmtNode) byte {
	switch node.(type) {
	case *ast.SelectStmt:
		return TypeSelect
	case *ast.SetOprStmt:
		return TypeSetOpr
	case *ast.DeleteStmt:
		return TypeDelete
	case *ast.UpdateStmt:
		return TypeUpdate
	case *ast.InsertStmt:
		return TypeInsert
	}
	return TypeInvalid
}

func (p *preprocessor) tableByName(tn *ast.TableName) (table.Table, error) {
	currentDB := p.sctx.GetSessionVars().CurrentDB
	if tn.Schema.String() != "" {
		currentDB = tn.Schema.L
	}
	if currentDB == "" {
		return nil, errors.Trace(plannererrors.ErrNoDB)
	}
	sName := ast.NewCIStr(currentDB)
	is := p.ensureInfoSchema()

	// for 'SHOW CREATE VIEW/SEQUENCE ...' statement, ignore local temporary tables.
	if p.stmtTp == TypeShow && (p.showTp == ast.ShowCreateView || p.showTp == ast.ShowCreateSequence) {
		is = temptable.DetachLocalTemporaryTableInfoSchema(is)
	}

	tbl, err := is.TableByName(p.ctx, sName, tn.Name)
	if err != nil {
		// We should never leak that the table doesn't exist (i.e. attachplannererrors.ErrTableNotExists)
		// unless we know that the user has permissions to it, should it exist.
		// By checking here, this makes all SELECT/SHOW/INSERT/UPDATE/DELETE statements safe.
		currentUser, activeRoles := p.sctx.GetSessionVars().User, p.sctx.GetSessionVars().ActiveRoles
		if pm := privilege.GetPrivilegeManager(p.sctx); pm != nil {
			if !pm.RequestVerification(activeRoles, sName.L, tn.Name.O, "", mysql.AllPrivMask) {
				u := currentUser.Username
				h := currentUser.Hostname
				if currentUser.AuthHostname != "" {
					u = currentUser.AuthUsername
					h = currentUser.AuthHostname
				}
				return nil, plannererrors.ErrTableaccessDenied.GenWithStackByArgs(p.stmtType(), u, h, tn.Name.O)
			}
		}
		return nil, err
	}
	return tbl, err
}

func (p *preprocessor) checkBindGrammar(originNode, hintedNode ast.StmtNode, defaultDB string) {
	origTp := bindableStmtType(originNode)
	hintedTp := bindableStmtType(hintedNode)
	if origTp == TypeInvalid || hintedTp == TypeInvalid {
		p.err = errors.Errorf("create binding doesn't support this type of query")
		return
	}
	if origTp != hintedTp {
		p.err = errors.Errorf("hinted sql and original sql have different query types")
		return
	}
	if origTp == TypeInsert {
		origInsert, hintedInsert := originNode.(*ast.InsertStmt), hintedNode.(*ast.InsertStmt)
		if origInsert.Select == nil || hintedInsert.Select == nil {
			p.err = errors.Errorf("create binding only supports INSERT / REPLACE INTO SELECT")
			return
		}
	}

	// Check the bind operation is not on any temporary table.
	nodeW := resolve.NewNodeWWithCtx(originNode, p.resolveCtx)
	tblNames := ExtractTableList(nodeW, false)
	for _, tn := range tblNames {
		tbl, err := p.tableByName(tn)
		if err != nil {
			// If the operation is order is: drop table -> drop binding
			// The table doesn't  exist, it is not an error.
			if terror.ErrorEqual(err, infoschema.ErrTableNotExists) {
				continue
			}
			p.err = err
			return
		}
		if tbl.Meta().TempTableType != model.TempTableNone {
			p.err = dbterror.ErrOptOnTemporaryTable.GenWithStackByArgs("create binding")
			return
		}
		tableInfo := tbl.Meta()
		dbInfo, _ := infoschema.SchemaByTable(p.ensureInfoSchema(), tableInfo)
		p.resolveCtx.AddTableName(&resolve.TableNameW{
			TableName: tn,
			DBInfo:    dbInfo,
			TableInfo: tableInfo,
		})
	}
	aliasChecker := &aliasChecker{}
	originNode.Accept(aliasChecker)
	hintedNode.Accept(aliasChecker)
	originSQL, _ := bindinfo.NormalizeStmtForBinding(originNode, defaultDB, false)
	hintedSQL, _ := bindinfo.NormalizeStmtForBinding(hintedNode, defaultDB, false)
	if originSQL != hintedSQL {
		p.err = errors.Errorf("hinted sql and origin sql don't match when hinted sql erase the hint info, after erase hint info, originSQL:%s, hintedSQL:%s", originSQL, hintedSQL)
	}
}

func (p *preprocessor) Leave(in ast.Node) (out ast.Node, ok bool) {
	switch x := in.(type) {
	case *ast.CreateTableStmt:
		p.flag &= ^inCreateOrDropTable
		p.checkAutoIncrement(x)
		p.checkContainDotColumn(x)
	case *ast.CreateViewStmt:
		p.flag &= ^inCreateOrDropTable
	case *ast.DropTableStmt, *ast.AlterTableStmt, *ast.RenameTableStmt:
		p.flag &= ^inCreateOrDropTable
	case *driver.ParamMarkerExpr:
		if p.flag&inPrepare == 0 {
			p.err = parser.ErrSyntax.GenWithStack("syntax error, unexpected '?'")
			return
		}
	case *ast.ExplainStmt:
		if _, ok := x.Stmt.(*ast.ShowStmt); ok {
			break
		}
		valid := false
		for i, length := 0, len(types.ExplainFormats); i < length; i++ {
			if strings.ToLower(x.Format) == types.ExplainFormats[i] {
				valid = true
				break
			}
		}
		if x.Format != "" && !valid {
			p.err = plannererrors.ErrUnknownExplainFormat.GenWithStackByArgs(x.Format)
		}
	case *ast.TableName:
		p.handleTableName(x)
	case *ast.Join:
		if len(p.tableAliasInJoin) > 0 {
			p.tableAliasInJoin = p.tableAliasInJoin[:len(p.tableAliasInJoin)-1]
		}
	case *ast.FuncCallExpr:
		// The arguments for builtin NAME_CONST should be constants
		// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_name-const for details
		if x.FnName.L == ast.NameConst {
			if len(x.Args) != 2 {
				p.err = expression.ErrIncorrectParameterCount.GenWithStackByArgs(x.FnName.L)
			} else {
				_, isValueExpr1 := x.Args[0].(*driver.ValueExpr)
				isValueExpr2 := false
				switch x.Args[1].(type) {
				case *driver.ValueExpr, *ast.UnaryOperationExpr:
					isValueExpr2 = true
				}

				if !isValueExpr1 || !isValueExpr2 {
					p.err = plannererrors.ErrWrongArguments.GenWithStackByArgs("NAME_CONST")
				}
			}
			break
		}

		// no need sleep when retry transaction and avoid unexpect sleep caused by retry.
		if p.flag&inTxnRetry > 0 && x.FnName.L == ast.Sleep {
			if len(x.Args) == 1 {
				x.Args[0] = ast.NewValueExpr(0, "", "")
			}
		}

		if x.FnName.L == ast.NextVal || x.FnName.L == ast.LastVal || x.FnName.L == ast.SetVal {
			p.flag &= ^inSequenceFunction
		}
	case *ast.RepairTableStmt:
		p.flag &= ^inRepairTable
	case *ast.CreateSequenceStmt:
		p.flag &= ^inCreateOrDropTable
	case *ast.BRIEStmt:
		if x.Kind == ast.BRIEKindRestore {
			p.flag &= ^inCreateOrDropTable
		}
	case *ast.CommonTableExpression, *ast.SubqueryExpr:
		with := p.preprocessWith
		lenWithCteBeforeOffset := len(with.cteBeforeOffset)
		if lenWithCteBeforeOffset < 1 {
			p.err = plannererrors.ErrInternal.GenWithStack("len(cteBeforeOffset) is less than one.Maybe it was deleted in somewhere.Should match in Enter and Leave")
			break
		}
		beforeOffset := with.cteBeforeOffset[lenWithCteBeforeOffset-1]
		with.cteBeforeOffset = with.cteBeforeOffset[:lenWithCteBeforeOffset-1]
		with.cteCanUsed = with.cteCanUsed[:beforeOffset]
		if cteNode, exist := x.(*ast.CommonTableExpression); exist {
			with.cteCanUsed = append(with.cteCanUsed, cteNode.Name.L)
		}
	case *ast.SelectStmt:
		if x.With != nil {
			p.preprocessWith.cteStack = p.preprocessWith.cteStack[0 : len(p.preprocessWith.cteStack)-1]
		}
	case *ast.SetOprStmt:
		if x.With != nil {
			p.preprocessWith.cteStack = p.preprocessWith.cteStack[0 : len(p.preprocessWith.cteStack)-1]
		}
	case *ast.SetOprSelectList:
		if x.With != nil {
			p.preprocessWith.cteStack = p.preprocessWith.cteStack[0 : len(p.preprocessWith.cteStack)-1]
		}
	}

	return in, p.err == nil
}

func checkAutoIncrementOp(colDef *ast.ColumnDef, index int) (bool, error) {
	var hasAutoIncrement bool

	if colDef.Options[index].Tp == ast.ColumnOptionAutoIncrement {
		hasAutoIncrement = true
		if len(colDef.Options) == index+1 {
			return hasAutoIncrement, nil
		}
		for _, op := range colDef.Options[index+1:] {
			if op.Tp == ast.ColumnOptionDefaultValue {
				if tmp, ok := op.Expr.(*driver.ValueExpr); ok {
					if !tmp.Datum.IsNull() {
						return hasAutoIncrement, types.ErrInvalidDefault.GenWithStackByArgs(colDef.Name.Name.O)
					}
				}
				if tmp, ok := op.Expr.(*ast.FuncCallExpr); ok {
					if tmp.FnName.L == "current_date" {
						return hasAutoIncrement, types.ErrInvalidDefault.GenWithStackByArgs(colDef.Name.Name.O)
					}
				}
			}
		}
	}

	if colDef.Options[index].Tp == ast.ColumnOptionDefaultValue && len(colDef.Options) != index+1 {
		if tmp, ok := colDef.Options[index].Expr.(*driver.ValueExpr); ok {
			if tmp.Datum.IsNull() {
				return hasAutoIncrement, nil
			}
		}
		for _, op := range colDef.Options[index+1:] {
			if op.Tp == ast.ColumnOptionAutoIncrement {
				return hasAutoIncrement, errors.Errorf("Invalid default value for '%s'", colDef.Name.Name.O)
			}
		}
	}

	return hasAutoIncrement, nil
}

func (p *preprocessor) checkAutoIncrement(stmt *ast.CreateTableStmt) {
	autoIncrementCols := make(map[*ast.ColumnDef]bool)

	for _, colDef := range stmt.Cols {
		var hasAutoIncrement bool
		var isKey bool
		for i, op := range colDef.Options {
			ok, err := checkAutoIncrementOp(colDef, i)
			if err != nil {
				p.err = err
				return
			}
			if ok {
				hasAutoIncrement = true
			}
			switch op.Tp {
			case ast.ColumnOptionPrimaryKey, ast.ColumnOptionUniqKey:
				isKey = true
			}
		}
		if hasAutoIncrement {
			autoIncrementCols[colDef] = isKey
		}
	}

	if len(autoIncrementCols) < 1 {
		return
	}
	// Only have one auto_increment col.
	if len(autoIncrementCols) > 1 {
		p.err = autoid.ErrWrongAutoKey.GenWithStackByArgs()
		return
	}
	for col := range autoIncrementCols {
		switch col.Tp.GetType() {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong,
			mysql.TypeFloat, mysql.TypeDouble, mysql.TypeLonglong, mysql.TypeInt24:
		default:
			p.err = errors.Errorf("Incorrect column specifier for column '%s'", col.Name.Name.O)
		}
	}
}

// checkSetOprSelectList checks union's selectList.
// refer: https://dev.mysql.com/doc/refman/5.7/en/union.html
//
//	https://mariadb.com/kb/en/intersect/
//	https://mariadb.com/kb/en/except/
//
// "To apply ORDER BY or LIMIT to an individual SELECT, place the clause inside the parentheses that enclose the SELECT."
func (p *preprocessor) checkSetOprSelectList(stmt *ast.SetOprSelectList) {
	for _, sel := range stmt.Selects[:len(stmt.Selects)-1] {
		switch s := sel.(type) {
		case *ast.SelectStmt:
			if s.SelectIntoOpt != nil {
				p.err = plannererrors.ErrWrongUsage.GenWithStackByArgs("UNION", "INTO")
				return
			}
			if s.IsInBraces {
				continue
			}
			if s.Limit != nil {
				p.err = plannererrors.ErrWrongUsage.GenWithStackByArgs("UNION", "LIMIT")
				return
			}
			if s.OrderBy != nil {
				p.err = plannererrors.ErrWrongUsage.GenWithStackByArgs("UNION", "ORDER BY")
				return
			}
		case *ast.SetOprSelectList:
			p.checkSetOprSelectList(s)
		}
	}
}

func (p *preprocessor) checkCreateDatabaseGrammar(stmt *ast.CreateDatabaseStmt) {
	if util.IsInCorrectIdentifierName(stmt.Name.L) {
		p.err = dbterror.ErrWrongDBName.GenWithStackByArgs(stmt.Name)
	}
}

func (p *preprocessor) checkAlterDatabaseGrammar(stmt *ast.AlterDatabaseStmt) {
	// for 'ALTER DATABASE' statement, database name can be empty to alter default database.
	if util.IsInCorrectIdentifierName(stmt.Name.L) && !stmt.AlterDefaultDatabase {
		p.err = dbterror.ErrWrongDBName.GenWithStackByArgs(stmt.Name)
	}
}

func (p *preprocessor) checkDropDatabaseGrammar(stmt *ast.DropDatabaseStmt) {
	if util.IsInCorrectIdentifierName(stmt.Name.L) {
		p.err = dbterror.ErrWrongDBName.GenWithStackByArgs(stmt.Name)
	}
}

func (p *preprocessor) checkFlashbackTableGrammar(stmt *ast.FlashBackTableStmt) {
	if util.IsInCorrectIdentifierName(stmt.NewName) {
		p.err = dbterror.ErrWrongTableName.GenWithStackByArgs(stmt.NewName)
	}
}

func (p *preprocessor) checkFlashbackDatabaseGrammar(stmt *ast.FlashBackDatabaseStmt) {
	if util.IsInCorrectIdentifierName(stmt.NewName) {
		p.err = dbterror.ErrWrongDBName.GenWithStackByArgs(stmt.NewName)
	}
}

func (p *preprocessor) checkAdminCheckTableGrammar(stmt *ast.AdminStmt) {
	for _, table := range stmt.Tables {
		tableInfo, err := p.tableByName(table)
		if err != nil {
			p.err = err
			return
		}
		tempTableType := tableInfo.Meta().TempTableType
		if (stmt.Tp == ast.AdminCheckTable || stmt.Tp == ast.AdminChecksumTable || stmt.Tp == ast.AdminCheckIndex) && tempTableType != model.TempTableNone {
			if stmt.Tp == ast.AdminChecksumTable {
				p.err = plannererrors.ErrOptOnTemporaryTable.GenWithStackByArgs("admin checksum table")
			} else if stmt.Tp == ast.AdminCheckTable {
				p.err = plannererrors.ErrOptOnTemporaryTable.GenWithStackByArgs("admin check table")
			} else {
				p.err = plannererrors.ErrOptOnTemporaryTable.GenWithStackByArgs("admin check index")
			}
			return
		}
	}
}

func (p *preprocessor) checkCreateTableGrammar(stmt *ast.CreateTableStmt) {
	if stmt.ReferTable != nil {
		schema := ast.NewCIStr(p.sctx.GetSessionVars().CurrentDB)
		if stmt.ReferTable.Schema.String() != "" {
			schema = stmt.ReferTable.Schema
		}
		// get the infoschema from the context.
		tableInfo, err := p.ensureInfoSchema().TableByName(p.ctx, schema, stmt.ReferTable.Name)
		if err != nil {
			p.err = err
			return
		}
		tableMetaInfo := tableInfo.Meta()
		if tableMetaInfo.TempTableType != model.TempTableNone {
			p.err = plannererrors.ErrOptOnTemporaryTable.GenWithStackByArgs("create table like")
			return
		}
		if stmt.TemporaryKeyword != ast.TemporaryNone {
			err := checkReferInfoForTemporaryTable(tableMetaInfo)
			if err != nil {
				p.err = err
				return
			}
		}
	}
	if stmt.TemporaryKeyword != ast.TemporaryNone {
		for _, opt := range stmt.Options {
			switch opt.Tp {
			case ast.TableOptionShardRowID:
				p.err = plannererrors.ErrOptOnTemporaryTable.GenWithStackByArgs("shard_row_id_bits")
				return
			case ast.TableOptionPlacementPolicy:
				p.err = plannererrors.ErrOptOnTemporaryTable.GenWithStackByArgs("PLACEMENT")
				return
			}
		}
	}
	tName := stmt.Table.Name.String()
	if util.IsInCorrectIdentifierName(tName) {
		p.err = dbterror.ErrWrongTableName.GenWithStackByArgs(tName)
		return
	}
	countPrimaryKey := 0
	for _, colDef := range stmt.Cols {
		if err := checkColumn(colDef); err != nil {
			// Try to convert to BLOB or TEXT, see issue #30328
			if !terror.ErrorEqual(err, types.ErrTooBigFieldLength) || !p.hasAutoConvertWarning(colDef) {
				p.err = err
				return
			}
		}
		isPrimary, err := checkColumnOptions(stmt.TemporaryKeyword != ast.TemporaryNone, colDef.Options)
		if err != nil {
			p.err = err
			return
		}
		countPrimaryKey += isPrimary
		if countPrimaryKey > 1 {
			p.err = infoschema.ErrMultiplePriKey
			return
		}
	}
	for _, constraint := range stmt.Constraints {
		switch tp := constraint.Tp; tp {
		case ast.ConstraintKey, ast.ConstraintIndex, ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex, ast.ConstraintForeignKey:
			err := checkIndexInfo(constraint.Name, constraint.Keys)
			if err != nil {
				p.err = err
				return
			}
			if constraint.IsEmptyIndex {
				p.err = dbterror.ErrWrongNameForIndex.GenWithStackByArgs(constraint.Name)
				return
			}
		case ast.ConstraintPrimaryKey:
			if countPrimaryKey > 0 {
				p.err = infoschema.ErrMultiplePriKey
				return
			}
			countPrimaryKey++
			err := checkIndexInfo(constraint.Name, constraint.Keys)
			if err != nil {
				p.err = err
				return
			}
		}
	}
	if p.err = checkUnsupportedTableOptions(stmt.Options); p.err != nil {
		return
	}
	if stmt.Select != nil {
		// FIXME: a temp error noticing 'not implemented' (issue 4754)
		// Note: if we implement it later, please clear it's MDL related tables for
		// it like what CREATE VIEW does.
		p.err = errors.New("'CREATE TABLE ... SELECT' is not implemented yet")
		return
	} else if len(stmt.Cols) == 0 && stmt.ReferTable == nil {
		p.err = dbterror.ErrTableMustHaveColumns
		return
	}

	if stmt.Partition != nil {
		for _, def := range stmt.Partition.Definitions {
			pName := def.Name.String()
			if util.IsInCorrectIdentifierName(pName) {
				p.err = dbterror.ErrWrongPartitionName.GenWithStackByArgs()
				return
			}
		}
	}
}

func (p *preprocessor) checkCreateViewGrammar(stmt *ast.CreateViewStmt) {
	vName := stmt.ViewName.Name.String()
	if util.IsInCorrectIdentifierName(vName) {
		p.err = dbterror.ErrWrongTableName.GenWithStackByArgs(vName)
		return
	}
	for _, col := range stmt.Cols {
		if util.IsInCorrectIdentifierName(col.String()) {
			p.err = dbterror.ErrWrongColumnName.GenWithStackByArgs(col)
			return
		}
	}
	if len(stmt.Definer.Username) > auth.UserNameMaxLength {
		p.err = dbterror.ErrWrongStringLength.GenWithStackByArgs(stmt.Definer.Username, "user name", auth.UserNameMaxLength)
		return
	}
	if len(stmt.Definer.Hostname) > auth.HostNameMaxLength {
		p.err = dbterror.ErrWrongStringLength.GenWithStackByArgs(stmt.Definer.Hostname, "host name", auth.HostNameMaxLength)
		return
	}
}

func (p *preprocessor) checkCreateViewWithSelect(stmt ast.Node) {
	switch s := stmt.(type) {
	case *ast.SelectStmt:
		if s.SelectIntoOpt != nil {
			p.err = dbterror.ErrViewSelectClause.GenWithStackByArgs("INFO")
			return
		}
		if s.LockInfo != nil && s.LockInfo.LockType != ast.SelectLockNone {
			s.LockInfo.LockType = ast.SelectLockNone
			return
		}
	case *ast.SetOprSelectList:
		for _, sel := range s.Selects {
			p.checkCreateViewWithSelect(sel)
		}
	}
}

func (p *preprocessor) checkCreateViewWithSelectGrammar(stmt *ast.CreateViewStmt) {
	switch stmt := stmt.Select.(type) {
	case *ast.SelectStmt:
		p.checkCreateViewWithSelect(stmt)
	case *ast.SetOprStmt:
		for _, selectStmt := range stmt.SelectList.Selects {
			p.checkCreateViewWithSelect(selectStmt)
			if p.err != nil {
				return
			}
		}
	}
}

func (p *preprocessor) checkDropSequenceGrammar(stmt *ast.DropSequenceStmt) {
	p.checkDropTableNames(stmt.Sequences)
}

func (p *preprocessor) checkDropTableGrammar(stmt *ast.DropTableStmt) {
	p.checkDropTableNames(stmt.Tables)
	if stmt.TemporaryKeyword != ast.TemporaryNone {
		p.checkDropTemporaryTableGrammar(stmt)
	}
}

func (p *preprocessor) checkDropTemporaryTableGrammar(stmt *ast.DropTableStmt) {
	currentDB := ast.NewCIStr(p.sctx.GetSessionVars().CurrentDB)
	for _, t := range stmt.Tables {
		if util.IsInCorrectIdentifierName(t.Name.String()) {
			p.err = dbterror.ErrWrongTableName.GenWithStackByArgs(t.Name.String())
			return
		}

		schema := t.Schema
		if schema.L == "" {
			schema = currentDB
		}

		tbl, err := p.ensureInfoSchema().TableByName(p.ctx, schema, t.Name)
		if infoschema.ErrTableNotExists.Equal(err) {
			// Non-exist table will be checked in ddl executor
			continue
		}

		if err != nil {
			p.err = err
			return
		}

		tblInfo := tbl.Meta()
		if stmt.TemporaryKeyword == ast.TemporaryGlobal && tblInfo.TempTableType != model.TempTableGlobal {
			p.err = plannererrors.ErrDropTableOnTemporaryTable
			return
		}
	}
}

func (p *preprocessor) checkDropTableNames(tables []*ast.TableName) {
	for _, t := range tables {
		if util.IsInCorrectIdentifierName(t.Name.String()) {
			p.err = dbterror.ErrWrongTableName.GenWithStackByArgs(t.Name.String())
			return
		}
	}
}

func (p *preprocessor) checkNonUniqTableAlias(stmt *ast.Join) {
	if p.flag&parentIsJoin == 0 {
		p.tableAliasInJoin = append(p.tableAliasInJoin, make(map[string]any))
	}
	tableAliases := p.tableAliasInJoin[len(p.tableAliasInJoin)-1]
	isOracleMode := p.sctx.GetSessionVars().SQLMode&mysql.ModeOracle != 0
	if !isOracleMode {
		if err := isTableAliasDuplicate(stmt.Left, tableAliases); err != nil {
			p.err = err
			return
		}
		if err := isTableAliasDuplicate(stmt.Right, tableAliases); err != nil {
			p.err = err
			return
		}
	}
	p.flag |= parentIsJoin
}

func isTableAliasDuplicate(node ast.ResultSetNode, tableAliases map[string]any) error {
	if ts, ok := node.(*ast.TableSource); ok {
		tabName := ts.AsName
		if tabName.L == "" {
			if tableNode, ok := ts.Source.(*ast.TableName); ok {
				if tableNode.Schema.L != "" {
					tabName = ast.NewCIStr(fmt.Sprintf("%s.%s", tableNode.Schema.L, tableNode.Name.L))
				} else {
					tabName = tableNode.Name
				}
			}
		}
		_, exists := tableAliases[tabName.L]
		if len(tabName.L) != 0 && exists {
			return plannererrors.ErrNonUniqTable.GenWithStackByArgs(tabName)
		}
		tableAliases[tabName.L] = nil
	}
	return nil
}

func checkColumnOptions(isTempTable bool, ops []*ast.ColumnOption) (int, error) {
	isPrimary, isGenerated, isStored := 0, 0, false

	for _, op := range ops {
		switch op.Tp {
		case ast.ColumnOptionPrimaryKey:
			isPrimary = 1
		case ast.ColumnOptionGenerated:
			isGenerated = 1
			isStored = op.Stored
		case ast.ColumnOptionAutoRandom:
			if isTempTable {
				return isPrimary, plannererrors.ErrOptOnTemporaryTable.GenWithStackByArgs("auto_random")
			}
		}
	}

	if isPrimary > 0 && isGenerated > 0 && !isStored {
		return isPrimary, plannererrors.ErrUnsupportedOnGeneratedColumn.GenWithStackByArgs("Defining a virtual generated column as primary key")
	}

	return isPrimary, nil
}

func checkIndexOptions(isColumnar bool, indexOptions *ast.IndexOption) error {
	if isColumnar && indexOptions == nil {
		return dbterror.ErrUnsupportedIndexType.FastGen("COLUMNAR INDEX must specify 'USING <index_type>'")
	}
	if indexOptions == nil {
		return nil
	}
	if isColumnar {
		switch indexOptions.Tp {
		case ast.IndexTypeVector, ast.IndexTypeInverted:
			// Accepted
		case ast.IndexTypeFulltext:
			if indexOptions.ParserName.L != "" && model.GetFullTextParserTypeBySQLName(indexOptions.ParserName.L) == model.FullTextParserTypeInvalid {
				return dbterror.ErrUnsupportedIndexType.FastGen("Unsupported parser '%s'", indexOptions.ParserName.O)
			}
		case ast.IndexTypeInvalid:
			return dbterror.ErrUnsupportedIndexType.FastGen("COLUMNAR INDEX must specify 'USING <index_type>'")
		default:
			return dbterror.ErrUnsupportedIndexType.FastGen("'USING %s' is not supported for COLUMNAR INDEX", indexOptions.Tp)
		}
		if indexOptions.Visibility == ast.IndexVisibilityInvisible {
			return dbterror.ErrUnsupportedIndexType.FastGen("INVISIBLE can not be used in %s INDEX", indexOptions.Tp)
		}
	} else {
		switch indexOptions.Tp {
		case ast.IndexTypeHNSW:
			return dbterror.ErrUnsupportedIndexType.FastGen("'USING HNSW' can be only used for VECTOR INDEX")
		case ast.IndexTypeVector, ast.IndexTypeInverted, ast.IndexTypeFulltext:
			return dbterror.ErrUnsupportedIndexType.FastGen("'USING %s' can be only used for COLUMNAR INDEX", indexOptions.Tp)
		default:
			// Accepted
		}
	}

	return nil
}

func checkIndexSpecs(indexOptions *ast.IndexOption, partSpecs []*ast.IndexPartSpecification) error {
	if indexOptions == nil {
		return nil
	}
	switch indexOptions.Tp {
	case ast.IndexTypeVector:
		if len(partSpecs) != 1 || partSpecs[0].Expr == nil {
			return dbterror.ErrUnsupportedAddColumnarIndex.FastGen("VECTOR INDEX must specify an expression like ((VEC_XX_DISTANCE(<COLUMN>)))")
		}
	case ast.IndexTypeInverted:
		if len(partSpecs) != 1 || partSpecs[0].Column == nil {
			return dbterror.ErrUnsupportedAddColumnarIndex.FastGen("COLUMNAR INDEX of INVERTED type must specify one column name")
		}
	case ast.IndexTypeFulltext:
		if len(partSpecs) != 1 || partSpecs[0].Column == nil {
			return dbterror.ErrUnsupportedAddColumnarIndex.FastGen("FULLTEXT index must specify one column name")
		}
	}
	return nil
}

func (p *preprocessor) checkCreateIndexGrammar(stmt *ast.CreateIndexStmt) {
	tName := stmt.Table.Name.String()
	if util.IsInCorrectIdentifierName(tName) {
		p.err = dbterror.ErrWrongTableName.GenWithStackByArgs(tName)
		return
	}
	if stmt.IndexName == "" {
		p.err = dbterror.ErrWrongNameForIndex.GenWithStackByArgs(stmt.IndexName)
		return
	}
	p.err = checkIndexInfo(stmt.IndexName, stmt.IndexPartSpecifications)
	if p.err != nil {
		return
	}

	// Rewrite CREATE VECTOR INDEX into CREATE COLUMNAR INDEX
	if stmt.KeyType == ast.IndexKeyTypeVector {
		if stmt.IndexOption.Tp != ast.IndexTypeInvalid && stmt.IndexOption.Tp != ast.IndexTypeHNSW {
			p.err = dbterror.ErrUnsupportedIndexType.FastGen("'USING %s' is not supported for VECTOR INDEX", stmt.IndexOption.Tp)
			return
		}
		stmt.KeyType = ast.IndexKeyTypeColumnar
		stmt.IndexOption.Tp = ast.IndexTypeVector
	}
	// Rewrite CREATE FULLTEXT INDEX into CREATE COLUMNAR INDEX
	if stmt.KeyType == ast.IndexKeyTypeFulltext {
		if stmt.IndexOption.Tp != ast.IndexTypeInvalid {
			p.err = dbterror.ErrUnsupportedIndexType.FastGen("'USING %s' is not supported for FULLTEXT INDEX", stmt.IndexOption.Tp)
			return
		}
		stmt.KeyType = ast.IndexKeyTypeColumnar
		stmt.IndexOption.Tp = ast.IndexTypeFulltext
	}

	p.err = checkIndexOptions(stmt.KeyType == ast.IndexKeyTypeColumnar, stmt.IndexOption)
	if p.err != nil {
		return
	}
	p.err = checkIndexSpecs(stmt.IndexOption, stmt.IndexPartSpecifications)
}

func (p *preprocessor) checkConstraintGrammar(stmt *ast.Constraint) {
	// Rewrite VECTOR INDEX into COLUMNAR INDEX
	if stmt.Tp == ast.ConstraintVector {
		if stmt.Option.Tp != ast.IndexTypeInvalid && stmt.Option.Tp != ast.IndexTypeHNSW {
			p.err = dbterror.ErrUnsupportedIndexType.FastGen("'USING %s' is not supported for VECTOR INDEX", stmt.Option.Tp)
			return
		}
		stmt.Tp = ast.ConstraintColumnar
		stmt.Option.Tp = ast.IndexTypeVector
	}
	if stmt.Tp == ast.ConstraintFulltext {
		if stmt.Option.Tp != ast.IndexTypeInvalid {
			p.err = dbterror.ErrUnsupportedIndexType.FastGen("'USING %s' is not supported for FULLTEXT INDEX", stmt.Option.Tp)
			return
		}
		stmt.Tp = ast.ConstraintColumnar
		stmt.Option.Tp = ast.IndexTypeFulltext
	}

	p.err = checkIndexOptions(stmt.Tp == ast.ConstraintColumnar, stmt.Option)
	if p.err != nil {
		return
	}
	p.err = checkIndexSpecs(stmt.Option, stmt.Keys)
}

func (p *preprocessor) checkSelectNoopFuncs(stmt *ast.SelectStmt) {
	noopFuncsMode := p.sctx.GetSessionVars().NoopFuncsMode
	if noopFuncsMode == variable.OnInt {
		// Set `ForShareLockEnabledByNoop` properly before returning.
		// When `tidb_enable_shared_lock_promotion` is enabled, the `for share` statements would be
		// executed as `for update` statements despite setting of noop functions.
		if stmt.LockInfo != nil && (stmt.LockInfo.LockType == ast.SelectLockForShare ||
			stmt.LockInfo.LockType == ast.SelectLockForShareNoWait) &&
			!p.sctx.GetSessionVars().SharedLockPromotion {
			p.sctx.GetSessionVars().StmtCtx.ForShareLockEnabledByNoop = true
		}
		return
	}
	if stmt.SelectStmtOpts != nil && stmt.SelectStmtOpts.CalcFoundRows {
		err := expression.ErrFunctionsNoopImpl.GenWithStackByArgs("SQL_CALC_FOUND_ROWS")
		if noopFuncsMode == variable.OffInt {
			p.err = err
			return
		}
		// NoopFuncsMode is Warn, append an error
		p.sctx.GetSessionVars().StmtCtx.AppendWarning(err)
	}

	// When `tidb_enable_shared_lock_promotion` is enabled, the `for share` statements would be
	// executed as `for update` statements.
	if stmt.LockInfo != nil && (stmt.LockInfo.LockType == ast.SelectLockForShare ||
		stmt.LockInfo.LockType == ast.SelectLockForShareNoWait) &&
		!p.sctx.GetSessionVars().SharedLockPromotion {
		err := expression.ErrFunctionsNoopImpl.GenWithStackByArgs("LOCK IN SHARE MODE")
		if noopFuncsMode == variable.OffInt {
			p.err = err
			return
		}
		// NoopFuncsMode is Warn, append an error
		p.sctx.GetSessionVars().StmtCtx.AppendWarning(err)
		p.sctx.GetSessionVars().StmtCtx.ForShareLockEnabledByNoop = true
	}
}

func (p *preprocessor) checkGroupBy(stmt *ast.GroupByClause) {
	noopFuncsMode := p.sctx.GetSessionVars().NoopFuncsMode
	for _, item := range stmt.Items {
		if !item.NullOrder && noopFuncsMode != variable.OnInt {
			err := expression.ErrFunctionsNoopImpl.GenWithStackByArgs("GROUP BY expr ASC|DESC")
			if noopFuncsMode == variable.OffInt {
				p.err = err
				return
			}
			// NoopFuncsMode is Warn, append an error
			p.sctx.GetSessionVars().StmtCtx.AppendWarning(err)
		}
	}
}

func (p *preprocessor) checkRenameTableGrammar(stmt *ast.RenameTableStmt) {
	oldTable := stmt.TableToTables[0].OldTable.Name.String()
	newTable := stmt.TableToTables[0].NewTable.Name.String()

	p.checkRenameTable(oldTable, newTable)
}

func (p *preprocessor) checkRenameTable(oldTable, newTable string) {
	if util.IsInCorrectIdentifierName(oldTable) {
		p.err = dbterror.ErrWrongTableName.GenWithStackByArgs(oldTable)
		return
	}

	if util.IsInCorrectIdentifierName(newTable) {
		p.err = dbterror.ErrWrongTableName.GenWithStackByArgs(newTable)
		return
	}
}

func (p *preprocessor) checkRepairTableGrammar(stmt *ast.RepairTableStmt) {
	// Check create table stmt whether it's is in REPAIR MODE.
	if !domainutil.RepairInfo.InRepairMode() {
		p.err = dbterror.ErrRepairTableFail.GenWithStackByArgs("TiDB is not in REPAIR MODE")
		return
	}
	if len(domainutil.RepairInfo.GetRepairTableList()) == 0 {
		p.err = dbterror.ErrRepairTableFail.GenWithStackByArgs("repair list is empty")
		return
	}

	// Check rename action as the rename statement does.
	oldTable := stmt.Table.Name.String()
	newTable := stmt.CreateStmt.Table.Name.String()
	p.checkRenameTable(oldTable, newTable)
}

func (p *preprocessor) checkAlterTableGrammar(stmt *ast.AlterTableStmt) {
	tName := stmt.Table.Name.String()
	if util.IsInCorrectIdentifierName(tName) {
		p.err = dbterror.ErrWrongTableName.GenWithStackByArgs(tName)
		return
	}
	specs := stmt.Specs
	for _, spec := range specs {
		if spec.NewTable != nil {
			ntName := spec.NewTable.Name.String()
			if util.IsInCorrectIdentifierName(ntName) {
				p.err = dbterror.ErrWrongTableName.GenWithStackByArgs(ntName)
				return
			}
		}
		for _, colDef := range spec.NewColumns {
			if p.err = checkColumn(colDef); p.err != nil {
				return
			}
		}
		if p.err = checkUnsupportedTableOptions(spec.Options); p.err != nil {
			return
		}
		switch spec.Tp {
		case ast.AlterTableAddConstraint:
			switch spec.Constraint.Tp {
			case ast.ConstraintKey, ast.ConstraintIndex, ast.ConstraintUniq, ast.ConstraintUniqIndex,
				ast.ConstraintUniqKey, ast.ConstraintPrimaryKey:
				p.err = checkIndexInfo(spec.Constraint.Name, spec.Constraint.Keys)
				if p.err != nil {
					return
				}
			default:
				// Nothing to do now.
			}
		case ast.AlterTableAddStatistics, ast.AlterTableDropStatistics:
			statsName := spec.Statistics.StatsName
			if util.IsInCorrectIdentifierName(statsName) {
				msg := fmt.Sprintf("Incorrect statistics name: %s", statsName)
				p.err = plannererrors.ErrInternal.GenWithStack(msg)
				return
			}
		case ast.AlterTableAddPartitions:
			for _, def := range spec.PartDefinitions {
				pName := def.Name.String()
				if util.IsInCorrectIdentifierName(pName) {
					p.err = dbterror.ErrWrongPartitionName.GenWithStackByArgs()
					return
				}
			}
		default:
			// Nothing to do now.
		}
	}
}

// checkDuplicateColumnName checks if index exists duplicated columns.
func checkDuplicateColumnName(indexPartSpecifications []*ast.IndexPartSpecification) error {
	colNames := make(map[string]struct{}, len(indexPartSpecifications))
	for _, IndexColNameWithExpr := range indexPartSpecifications {
		if IndexColNameWithExpr.Column != nil {
			name := IndexColNameWithExpr.Column.Name
			if _, ok := colNames[name.L]; ok {
				return infoschema.ErrColumnExists.GenWithStackByArgs(name)
			}
			colNames[name.L] = struct{}{}
		}
	}
	return nil
}

// checkIndexInfo checks index name, index column names and prefix lengths.
func checkIndexInfo(indexName string, indexPartSpecifications []*ast.IndexPartSpecification) error {
	if strings.EqualFold(indexName, mysql.PrimaryKeyName) {
		return dbterror.ErrWrongNameForIndex.GenWithStackByArgs(indexName)
	}
	if len(indexPartSpecifications) > mysql.MaxKeyParts {
		return infoschema.ErrTooManyKeyParts.GenWithStackByArgs(mysql.MaxKeyParts)
	}
	for _, idxSpec := range indexPartSpecifications {
		// -1 => unspecified/full, > 0 OK, 0 => error
		if idxSpec.Expr == nil && idxSpec.Length == 0 {
			return plannererrors.ErrKeyPart0.GenWithStackByArgs(idxSpec.Column.Name.O)
		}
	}
	return checkDuplicateColumnName(indexPartSpecifications)
}

// checkUnsupportedTableOptions checks if there exists unsupported table options
func checkUnsupportedTableOptions(options []*ast.TableOption) error {
	var err error
	for _, option := range options {
		switch option.Tp {
		case ast.TableOptionUnion:
			err = dbterror.ErrTableOptionUnionUnsupported
		case ast.TableOptionInsertMethod:
			err = dbterror.ErrTableOptionInsertMethodUnsupported
		case ast.TableOptionEngine:
			err = checkTableEngine(option.StrValue)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

var mysqlValidTableEngineNames = map[string]struct{}{
	"archive":    {},
	"blackhole":  {},
	"csv":        {},
	"example":    {},
	"federated":  {},
	"innodb":     {},
	"memory":     {},
	"merge":      {},
	"mgr_myisam": {},
	"myisam":     {},
	"ndb":        {},
	"heap":       {},
}

func checkTableEngine(engineName string) error {
	if _, have := mysqlValidTableEngineNames[strings.ToLower(engineName)]; !have {
		return dbterror.ErrUnknownEngine.GenWithStackByArgs(engineName)
	}
	return nil
}

func checkReferInfoForTemporaryTable(tableMetaInfo *model.TableInfo) error {
	if tableMetaInfo.AutoRandomBits != 0 {
		return plannererrors.ErrOptOnTemporaryTable.GenWithStackByArgs("auto_random")
	}
	if tableMetaInfo.PreSplitRegions != 0 {
		return plannererrors.ErrOptOnTemporaryTable.GenWithStackByArgs("pre split regions")
	}
	if tableMetaInfo.Partition != nil {
		return plannererrors.ErrPartitionNoTemporary
	}
	if tableMetaInfo.ShardRowIDBits != 0 {
		return plannererrors.ErrOptOnTemporaryTable.GenWithStackByArgs("shard_row_id_bits")
	}
	if tableMetaInfo.PlacementPolicyRef != nil {
		return plannererrors.ErrOptOnTemporaryTable.GenWithStackByArgs("placement")
	}

	return nil
}

// checkColumn checks if the column definition is valid.
// See https://dev.mysql.com/doc/refman/5.7/en/storage-requirements.html
func checkColumn(colDef *ast.ColumnDef) error {
	// Check column name.
	cName := colDef.Name.Name.String()
	if util.IsInCorrectIdentifierName(cName) {
		return dbterror.ErrWrongColumnName.GenWithStackByArgs(cName)
	}

	if isInvalidDefaultValue(colDef) {
		return types.ErrInvalidDefault.GenWithStackByArgs(colDef.Name.Name.O)
	}

	// Check column type.
	tp := colDef.Tp
	if tp == nil {
		return nil
	}
	if tp.GetFlen() > math.MaxUint32 {
		return types.ErrTooBigDisplayWidth.GenWithStack("Display width out of range for column '%s' (max = %d)", colDef.Name.Name.O, math.MaxUint32)
	}

	switch tp.GetType() {
	case mysql.TypeString:
		if tp.GetFlen() != types.UnspecifiedLength && tp.GetFlen() > mysql.MaxFieldCharLength {
			return types.ErrTooBigFieldLength.GenWithStack("Column length too big for column '%s' (max = %d); use BLOB or TEXT instead", colDef.Name.Name.O, mysql.MaxFieldCharLength)
		}
	case mysql.TypeVarchar:
		if len(tp.GetCharset()) == 0 {
			// It's not easy to get the schema charset and table charset here.
			// The charset is determined by the order ColumnDefaultCharset --> TableDefaultCharset-->DatabaseDefaultCharset-->SystemDefaultCharset.
			// return nil, to make the check in the ddl.CreateTable.
			return nil
		}
		err := types.IsVarcharTooBigFieldLength(colDef.Tp.GetFlen(), colDef.Name.Name.O, tp.GetCharset())
		if err != nil {
			return err
		}
	case mysql.TypeFloat, mysql.TypeDouble:
		// For FLOAT, the SQL standard permits an optional specification of the precision.
		// https://dev.mysql.com/doc/refman/8.0/en/floating-point-types.html
		if tp.GetDecimal() == -1 {
			switch tp.GetType() {
			case mysql.TypeDouble:
				// For Double type flen and decimal check is moved to parser component
			default:
				if tp.GetFlen() > mysql.MaxDoublePrecisionLength {
					return types.ErrWrongFieldSpec.GenWithStackByArgs(colDef.Name.Name.O)
				}
			}
		} else {
			if tp.GetDecimal() > mysql.MaxFloatingTypeScale {
				return types.ErrTooBigScale.GenWithStackByArgs(tp.GetDecimal(), colDef.Name.Name.O, mysql.MaxFloatingTypeScale)
			}
			if tp.GetFlen() > mysql.MaxFloatingTypeWidth || tp.GetFlen() == 0 {
				return types.ErrTooBigDisplayWidth.GenWithStackByArgs(colDef.Name.Name.O, mysql.MaxFloatingTypeWidth)
			}
			if tp.GetFlen() < tp.GetDecimal() {
				return types.ErrMBiggerThanD.GenWithStackByArgs(colDef.Name.Name.O)
			}
		}
	case mysql.TypeSet:
		if len(tp.GetElems()) > mysql.MaxTypeSetMembers {
			return types.ErrTooBigSet.GenWithStack("Too many strings for column %s and SET", colDef.Name.Name.O)
		}
		// Check set elements. See https://dev.mysql.com/doc/refman/5.7/en/set.html.
		for _, str := range colDef.Tp.GetElems() {
			if strings.Contains(str, ",") {
				return types.ErrIllegalValueForType.GenWithStackByArgs(types.TypeStr(tp.GetType()), str)
			}
		}
	case mysql.TypeNewDecimal:
		tpFlen := tp.GetFlen()
		tpDecimal := tp.GetDecimal()
		if tpDecimal > mysql.MaxDecimalScale {
			return types.ErrTooBigScale.GenWithStackByArgs(tpDecimal, colDef.Name.Name.O, mysql.MaxDecimalScale)
		}
		if tpFlen > mysql.MaxDecimalWidth {
			return types.ErrTooBigPrecision.GenWithStackByArgs(tpFlen, colDef.Name.Name.O, mysql.MaxDecimalWidth)
		}
		if tpFlen < tpDecimal {
			return types.ErrMBiggerThanD.GenWithStackByArgs(colDef.Name.Name.O)
		}
		// If decimal and flen all equals 0, just set flen to default value.
		if tpFlen == 0 && (tpDecimal == 0 || tpDecimal == types.UnspecifiedLength) {
			defaultFlen, _ := mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeNewDecimal)
			tp.SetFlen(defaultFlen)
			tp.SetDecimal(0)
		}
	case mysql.TypeBit:
		if tp.GetFlen() <= 0 {
			return types.ErrInvalidFieldSize.GenWithStackByArgs(colDef.Name.Name.O)
		}
		if tp.GetFlen() > mysql.MaxBitDisplayWidth {
			return types.ErrTooBigDisplayWidth.GenWithStackByArgs(colDef.Name.Name.O, mysql.MaxBitDisplayWidth)
		}
	case mysql.TypeTiDBVectorFloat32:
		if tp.GetFlen() != types.UnspecifiedLength {
			if err := types.CheckVectorDimValid(tp.GetFlen()); err != nil {
				return err
			}
		}
	default:
		// TODO: Add more types.
	}
	return nil
}

// isDefaultValNowSymFunc checks whether default value is a NOW() builtin function.
func isDefaultValNowSymFunc(expr ast.ExprNode) bool {
	if funcCall, ok := expr.(*ast.FuncCallExpr); ok {
		// Default value NOW() is transformed to CURRENT_TIMESTAMP() in parser.
		if funcCall.FnName.L == ast.CurrentTimestamp {
			return true
		}
	}
	return false
}

func isInvalidDefaultValue(colDef *ast.ColumnDef) bool {
	tp := colDef.Tp
	// Check the last default value.
	for i := len(colDef.Options) - 1; i >= 0; i-- {
		columnOpt := colDef.Options[i]
		if columnOpt.Tp == ast.ColumnOptionDefaultValue {
			if !(tp.GetType() == mysql.TypeTimestamp || tp.GetType() == mysql.TypeDatetime) && isDefaultValNowSymFunc(columnOpt.Expr) {
				return true
			}
			break
		}
	}

	return false
}

// checkContainDotColumn checks field contains the table name.
// for example :create table t (c1.c2 int default null).
func (p *preprocessor) checkContainDotColumn(stmt *ast.CreateTableStmt) {
	tName := stmt.Table.Name.String()
	sName := stmt.Table.Schema.String()

	for _, colDef := range stmt.Cols {
		// check schema and table names.
		if colDef.Name.Schema.O != sName && len(colDef.Name.Schema.O) != 0 {
			p.err = dbterror.ErrWrongDBName.GenWithStackByArgs(colDef.Name.Schema.O)
			return
		}
		if colDef.Name.Table.O != tName && len(colDef.Name.Table.O) != 0 {
			p.err = dbterror.ErrWrongTableName.GenWithStackByArgs(colDef.Name.Table.O)
			return
		}
	}
}

func (p *preprocessor) stmtType() string {
	switch p.stmtTp {
	case TypeDelete:
		return "DELETE"
	case TypeUpdate:
		return "UPDATE"
	case TypeInsert:
		return "INSERT"
	case TypeDrop:
		return "DROP"
	case TypeCreate:
		return "CREATE"
	case TypeAlter:
		return "ALTER"
	case TypeRename:
		return "DROP, ALTER"
	case TypeRepair:
		return "SELECT, INSERT"
	case TypeShow:
		return "SHOW"
	case TypeImportInto:
		return "IMPORT INTO"
	default:
		return "SELECT" // matches Select and uncaught cases.
	}
}

func (p *preprocessor) handleTableName(tn *ast.TableName) {
	if tn.Schema.L == "" {
		if slices.Contains(p.preprocessWith.cteCanUsed, tn.Name.L) {
			p.preprocessWith.UpdateCTEConsumerCount(tn.Name.L)
			return
		}

		currentDB := p.sctx.GetSessionVars().CurrentDB
		if currentDB == "" {
			p.err = errors.Trace(plannererrors.ErrNoDB)
			return
		}

		tn.Schema = ast.NewCIStr(currentDB)
	}

	if p.flag&inCreateOrDropTable > 0 {
		// The table may not exist in create table or drop table statement.
		if p.flag&inRepairTable > 0 {
			// Create stmt is in repair stmt, skip resolving the table to avoid error.
			return
		}
		// Create stmt is not in repair stmt, check the table not in repair list.
		if domainutil.RepairInfo.InRepairMode() {
			p.checkNotInRepair(tn)
		}
		return
	}
	// repairStmt: admin repair table A create table B ...
	// repairStmt's tableName is whether `inCreateOrDropTable` or `inRepairTable` flag.
	if p.flag&inRepairTable > 0 {
		p.handleRepairName(tn)
		return
	}

	if p.stmtTp == TypeSelect {
		if p.err = p.staleReadProcessor.OnSelectTable(tn); p.err != nil {
			return
		}
		if p.err = p.updateStateFromStaleReadProcessor(); p.err != nil {
			return
		}
	}

	table, err := p.tableByName(tn)
	if err != nil {
		p.err = err
		return
	}

	if !p.skipLockMDL() {
		table, err = tryLockMDLAndUpdateSchemaIfNecessary(p.ctx, p.sctx.GetPlanCtx(), ast.NewCIStr(tn.Schema.L), table, p.ensureInfoSchema())
		if err != nil {
			p.err = err
			return
		}
	}

	tableInfo := table.Meta()
	dbInfo, _ := infoschema.SchemaByTable(p.ensureInfoSchema(), tableInfo)
	// tableName should be checked as sequence object.
	if p.flag&inSequenceFunction > 0 {
		if !tableInfo.IsSequence() {
			p.err = infoschema.ErrWrongObject.GenWithStackByArgs(dbInfo.Name.O, tableInfo.Name.O, "SEQUENCE")
			return
		}
	}
	p.resolveCtx.AddTableName(&resolve.TableNameW{
		TableName: tn,
		DBInfo:    dbInfo,
		TableInfo: tableInfo,
	})
}

func (p *preprocessor) checkNotInRepair(tn *ast.TableName) {
	tableInfo, dbInfo := domainutil.RepairInfo.GetRepairedTableInfoByTableName(tn.Schema.L, tn.Name.L)
	if dbInfo == nil {
		return
	}
	if tableInfo != nil {
		p.err = dbterror.ErrWrongTableName.GenWithStackByArgs(tn.Name.L, "this table is in repair")
	}
}

func (p *preprocessor) handleRepairName(tn *ast.TableName) {
	// Check the whether the repaired table is system table.
	if metadef.IsMemOrSysDB(tn.Schema.L) {
		p.err = dbterror.ErrRepairTableFail.GenWithStackByArgs("memory or system database is not for repair")
		return
	}
	tableInfo, dbInfo := domainutil.RepairInfo.GetRepairedTableInfoByTableName(tn.Schema.L, tn.Name.L)
	// tableName here only has the schema rather than DBInfo.
	if dbInfo == nil {
		p.err = dbterror.ErrRepairTableFail.GenWithStackByArgs("database " + tn.Schema.L + " is not in repair")
		return
	}
	if tableInfo == nil {
		p.err = dbterror.ErrRepairTableFail.GenWithStackByArgs("table " + tn.Name.L + " is not in repair")
		return
	}
	p.sctx.SetValue(domainutil.RepairedTable, tableInfo)
	p.sctx.SetValue(domainutil.RepairedDatabase, dbInfo)
}

func (p *preprocessor) resolveShowStmt(node *ast.ShowStmt) {
	if node.DBName == "" {
		if node.Table != nil && node.Table.Schema.L != "" {
			node.DBName = node.Table.Schema.O
		} else {
			node.DBName = p.sctx.GetSessionVars().CurrentDB
		}
	} else if node.Table != nil && node.Table.Schema.L == "" {
		node.Table.Schema = ast.NewCIStr(node.DBName)
	}
	if node.User != nil && node.User.CurrentUser {
		// Fill the Username and Hostname with the current user.
		currentUser := p.sctx.GetSessionVars().User
		if currentUser != nil {
			node.User.Username = currentUser.Username
			node.User.Hostname = currentUser.Hostname
			node.User.AuthUsername = currentUser.AuthUsername
			node.User.AuthHostname = currentUser.AuthHostname
		}
	}
}

func (p *preprocessor) resolveExecuteStmt(node *ast.ExecuteStmt) {
	prepared, err := GetPreparedStmt(node, p.sctx.GetSessionVars())
	if err != nil {
		p.err = err
		return
	}

	if p.err = p.staleReadProcessor.OnExecutePreparedStmt(prepared.SnapshotTSEvaluator); p.err == nil {
		if p.err = p.updateStateFromStaleReadProcessor(); p.err != nil {
			return
		}
	}
}

func (*preprocessor) resolveCreateTableStmt(node *ast.CreateTableStmt) {
	for _, val := range node.Constraints {
		if val.Refer != nil && val.Refer.Table.Schema.String() == "" {
			val.Refer.Table.Schema = node.Table.Schema
		}
	}
}

func (p *preprocessor) resolveAlterTableStmt(node *ast.AlterTableStmt) {
	for _, spec := range node.Specs {
		if spec.Tp == ast.AlterTableRenameTable {
			p.flag |= inCreateOrDropTable
			break
		}
		if spec.Tp == ast.AlterTableAddConstraint && spec.Constraint.Refer != nil {
			table := spec.Constraint.Refer.Table
			if table.Schema.L == "" && node.Table.Schema.L != "" {
				table.Schema = ast.NewCIStr(node.Table.Schema.L)
			}
			if spec.Constraint.Tp == ast.ConstraintForeignKey {
				// when foreign_key_checks is off, should ignore err when refer table is not exists.
				p.flag |= inCreateOrDropTable
			}
		}
	}
}

func (p *preprocessor) resolveCreateSequenceStmt(stmt *ast.CreateSequenceStmt) {
	sName := stmt.Name.Name.String()
	if util.IsInCorrectIdentifierName(sName) {
		p.err = dbterror.ErrWrongTableName.GenWithStackByArgs(sName)
		return
	}
}

func (p *preprocessor) checkFuncCastExpr(node *ast.FuncCastExpr) {
	if node.Tp.EvalType() == types.ETDecimal {
		if node.Tp.GetFlen() >= node.Tp.GetDecimal() && node.Tp.GetFlen() <= mysql.MaxDecimalWidth && node.Tp.GetDecimal() <= mysql.MaxDecimalScale {
			// valid
			return
		}

		var buf strings.Builder
		restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &buf)
		if err := node.Expr.Restore(restoreCtx); err != nil {
			p.err = err
			return
		}
		if node.Tp.GetFlen() < node.Tp.GetDecimal() {
			p.err = types.ErrMBiggerThanD.GenWithStackByArgs(buf.String())
			return
		}
		if node.Tp.GetFlen() > mysql.MaxDecimalWidth {
			p.err = types.ErrTooBigPrecision.GenWithStackByArgs(node.Tp.GetFlen(), buf.String(), mysql.MaxDecimalWidth)
			return
		}
		if node.Tp.GetDecimal() > mysql.MaxDecimalScale {
			p.err = types.ErrTooBigScale.GenWithStackByArgs(node.Tp.GetDecimal(), buf.String(), mysql.MaxDecimalScale)
			return
		}
	}
	if node.Tp.EvalType() == types.ETDatetime {
		if node.Tp.GetDecimal() > types.MaxFsp {
			p.err = types.ErrTooBigPrecision.GenWithStackByArgs(node.Tp.GetDecimal(), "CAST", types.MaxFsp)
			return
		}
	}
}

func (p *preprocessor) updateStateFromStaleReadProcessor() error {
	if p.initedLastSnapshotTS {
		return nil
	}

	if p.IsStaleness = p.staleReadProcessor.IsStaleness(); p.IsStaleness {
		p.LastSnapshotTS = p.staleReadProcessor.GetStalenessReadTS()
		p.SnapshotTSEvaluator = p.staleReadProcessor.GetStalenessTSEvaluatorForPrepare()
		p.InfoSchema = p.staleReadProcessor.GetStalenessInfoSchema()
		p.InfoSchema = &infoschema.SessionExtendedInfoSchema{InfoSchema: p.InfoSchema}
		// If the select statement was like 'select * from t as of timestamp ...' or in a stale read transaction
		// or is affected by the tidb_read_staleness session variable, then the statement will be makred as isStaleness
		// in stmtCtx
		if p.flag&initTxnContextProvider != 0 {
			p.sctx.GetSessionVars().StmtCtx.IsStaleness = true
			if !p.sctx.GetSessionVars().InTxn() {
				txnManager := sessiontxn.GetTxnManager(p.sctx)
				newTxnRequest := &sessiontxn.EnterNewTxnRequest{
					Type:     sessiontxn.EnterNewTxnWithReplaceProvider,
					Provider: staleread.NewStalenessTxnContextProvider(p.sctx, p.LastSnapshotTS, p.InfoSchema),
				}
				if err := txnManager.EnterNewTxn(context.TODO(), newTxnRequest); err != nil {
					return err
				}
				if err := txnManager.OnStmtStart(context.TODO(), txnManager.GetCurrentStmt()); err != nil {
					return err
				}
				p.sctx.GetSessionVars().TxnCtx.StaleReadTs = p.LastSnapshotTS
			}
		}
	}
	p.initedLastSnapshotTS = true
	return nil
}

// ensureInfoSchema get the infoschema from the preprocessor.
// there some situations:
//   - the stmt specifies the schema version.
//   - session variable
//   - transaction context
func (p *preprocessor) ensureInfoSchema() infoschema.InfoSchema {
	if p.InfoSchema != nil {
		return p.InfoSchema
	}

	p.InfoSchema = sessiontxn.GetTxnManager(p.sctx).GetTxnInfoSchema()
	return p.InfoSchema
}

func (p *preprocessor) hasAutoConvertWarning(colDef *ast.ColumnDef) bool {
	sessVars := p.sctx.GetSessionVars()
	if !sessVars.SQLMode.HasStrictMode() && colDef.Tp.GetType() == mysql.TypeVarchar {
		colDef.Tp.SetType(mysql.TypeBlob)
		if colDef.Tp.GetCharset() == charset.CharsetBin {
			sessVars.StmtCtx.AppendWarning(dbterror.ErrAutoConvert.FastGenByArgs(colDef.Name.Name.O, "VARBINARY", "BLOB"))
		} else {
			sessVars.StmtCtx.AppendWarning(dbterror.ErrAutoConvert.FastGenByArgs(colDef.Name.Name.O, "VARCHAR", "TEXT"))
		}
		return true
	}
	return false
}

func tryLockMDLAndUpdateSchemaIfNecessary(ctx context.Context, sctx base.PlanContext, dbName ast.CIStr, tbl table.Table, is infoschema.InfoSchema) (retTbl table.Table, err error) {
	skipLock := false
	shouldLockMDL := false
	var lockedID int64

	defer func() {
		// if the table is not in public state, avoid running any queries on it. This verification is actually
		// not related to the MDL, but it's a good place to put it.
		//
		// This function may return a new table, so we need to check the return value in the `defer` block.
		if err == nil {
			if retTbl.Meta().State != model.StatePublic {
				err = infoschema.ErrTableNotExists.FastGenByArgs(dbName.L, retTbl.Meta().Name.L)
				retTbl = nil
			}
		}

		if shouldLockMDL && err == nil && !skipLock {
			sctx.GetSessionVars().StmtCtx.MDLRelatedTableIDs[retTbl.Meta().ID] = struct{}{}
		}

		if lockedID != 0 && err != nil {
			// because `err != nil`, the `retTbl` is `nil` and the `tbl` can also be `nil` here. We use the `lockedID` instead.
			sctx.GetSessionVars().GetRelatedTableForMDL().Delete(lockedID)
		}
	}()

	if !sctx.GetSessionVars().TxnCtx.EnableMDL {
		return tbl, nil
	}
	if is.SchemaMetaVersion() == 0 {
		return tbl, nil
	}
	if sctx.GetSessionVars().SnapshotInfoschema != nil {
		return tbl, nil
	}
	if sctx.GetSessionVars().TxnCtx.IsStaleness {
		return tbl, nil
	}
	if tbl.Meta().TempTableType == model.TempTableLocal {
		// Don't attach, don't lock.
		return tbl, nil
	} else if tbl.Meta().TempTableType == model.TempTableGlobal {
		skipLock = true
	}
	if IsAutoCommitTxn(sctx.GetSessionVars()) && sctx.GetSessionVars().StmtCtx.IsReadOnly {
		return tbl, nil
	}
	tableInfo := tbl.Meta()
	shouldLockMDL = true
	if _, ok := sctx.GetSessionVars().GetRelatedTableForMDL().Load(tableInfo.ID); !ok {
		if se, ok := is.(*infoschema.SessionExtendedInfoSchema); ok && skipLock && se.MdlTables != nil {
			if _, ok := se.MdlTables.TableByID(tableInfo.ID); ok {
				// Already attach.
				return tbl, nil
			}
		}

		// We need to write 0 to the map to block the txn.
		// If we don't write 0, consider the following case:
		// the background mdl check loop gets the mdl lock from this txn. But the domain infoSchema may be changed before writing the ver to the map.
		// In this case, this TiDB wrongly gets the mdl lock.
		if !skipLock {
			sctx.GetSessionVars().GetRelatedTableForMDL().Store(tableInfo.ID, int64(0))
			lockedID = tableInfo.ID
		}
		latestIS := sctx.GetLatestISWithoutSessExt().(infoschema.InfoSchema)
		domainSchemaVer := latestIS.SchemaMetaVersion()
		tbl, err = latestIS.TableByName(ctx, dbName, tableInfo.Name)
		if err != nil {
			return nil, err
		}
		if !skipLock {
			sctx.GetSessionVars().GetRelatedTableForMDL().Store(tbl.Meta().ID, domainSchemaVer)
			lockedID = tbl.Meta().ID
		}
		// Check the table change, if adding new public index or modify a column, we need to handle them.
		if tbl.Meta().Revision != tableInfo.Revision && !sctx.GetSessionVars().IsPessimisticReadConsistency() {
			var copyTableInfo *model.TableInfo

			infoIndices := make(map[string]int64, len(tableInfo.Indices))
			for _, idx := range tableInfo.Indices {
				infoIndices[idx.Name.L] = idx.ID
			}

			for i, idx := range tbl.Meta().Indices {
				if idx.State != model.StatePublic {
					continue
				}
				id, found := infoIndices[idx.Name.L]
				if !found || id != idx.ID {
					if copyTableInfo == nil {
						copyTableInfo = tbl.Meta().Clone()
					}
					copyTableInfo.Indices[i].State = model.StateWriteReorganization
					dbInfo, _ := latestIS.SchemaByName(dbName)
					allocs := autoid.NewAllocatorsFromTblInfo(latestIS.GetAutoIDRequirement(), dbInfo.ID, copyTableInfo)
					tbl, err = table.TableFromMeta(allocs, copyTableInfo)
					if err != nil {
						return nil, err
					}
				}
			}
			// Check the column change.
			infoColumns := make(map[string]int64, len(tableInfo.Columns))
			for _, col := range tableInfo.Columns {
				infoColumns[col.Name.L] = col.ID
			}
			for _, col := range tbl.Meta().Columns {
				if col.State != model.StatePublic {
					continue
				}
				colid, found := infoColumns[col.Name.L]
				if found && colid != col.ID {
					logutil.BgLogger().Info("public column changed",
						zap.String("column", col.Name.L), zap.String("old_col", col.Name.L),
						zap.Int64("new id", col.ID), zap.Int64("old id", col.ID))
					return nil, domain.ErrInfoSchemaChanged.GenWithStack("public column %s has changed", col.Name)
				}
			}
		}

		se, ok := is.(*infoschema.SessionExtendedInfoSchema)
		if !ok {
			logutil.BgLogger().Error("InfoSchema is not SessionExtendedInfoSchema", zap.Stack("stack"))
			return nil, errors.New("InfoSchema is not SessionExtendedInfoSchema")
		}
		db, _ := infoschema.SchemaByTable(latestIS, tbl.Meta())
		err = se.UpdateTableInfo(db, tbl)
		if err != nil {
			return nil, err
		}
		curTxn, err := sctx.Txn(false)
		if err != nil {
			return nil, err
		}
		if curTxn.Valid() {
			curTxn.SetOption(kv.TableToColumnMaps, nil)
		}
		return tbl, nil
	}
	return tbl, nil
}

// skipLockMDL returns true if the preprocessor should skip the lock of MDL.
func (p *preprocessor) skipLockMDL() bool {
	// skip lock mdl for IMPORT INTO statement,
	// because it's a batch process and will do both DML and DDL.
	// skip lock mdl for ANALYZE statement.
	return p.flag&inImportInto > 0 || p.flag&inAnalyze > 0
}

// aliasChecker is used to check the alias of the table in delete statement.
//
//	for example: delete tt1 from t1 tt1,(select max(id) id from t2)tt2 where tt1.id<=tt2.id
//	  `delete tt1` will be transformed to `delete current_database.t1` by default.
//	   because `tt1` cannot be used as alias in delete statement.
//	   so we have to set `tt1` as alias by aliasChecker.
type aliasChecker struct{}

func (*aliasChecker) Enter(in ast.Node) (ast.Node, bool) {
	if deleteStmt, ok := in.(*ast.DeleteStmt); ok {
		// 1. check the tableRefs of deleteStmt to find the alias
		var aliases []*ast.CIStr
		if deleteStmt.TableRefs != nil && deleteStmt.TableRefs.TableRefs != nil {
			tableRefs := deleteStmt.TableRefs.TableRefs
			if val := getTableRefsAlias(tableRefs.Left); val != nil {
				aliases = append(aliases, val)
			}
			if val := getTableRefsAlias(tableRefs.Right); val != nil {
				aliases = append(aliases, val)
			}
		}
		// 2. check the Tables to tag the alias
		if deleteStmt.Tables != nil && deleteStmt.Tables.Tables != nil {
			for _, table := range deleteStmt.Tables.Tables {
				if table.Schema.String() != "" {
					continue
				}
				for _, alias := range aliases {
					if table.Name.L == alias.L {
						table.IsAlias = true
						break
					}
				}
			}
		}
		return in, true
	}
	return in, false
}

func getTableRefsAlias(tableRefs ast.ResultSetNode) *ast.CIStr {
	switch v := tableRefs.(type) {
	case *ast.Join:
		if v.Left != nil {
			return getTableRefsAlias(v.Left)
		}
	case *ast.TableSource:
		return &v.AsName
	}
	return nil
}

func (*aliasChecker) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}
