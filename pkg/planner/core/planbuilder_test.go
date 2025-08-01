// Copyright 2017 PingCAP, Inc.
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
	"math/rand"
	"net/url"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"
	"unsafe"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/stretchr/testify/require"
)

type visit struct {
	a1  unsafe.Pointer
	a2  unsafe.Pointer
	typ reflect.Type
}

func TestShow(t *testing.T) {
	node := &ast.ShowStmt{}
	tps := []ast.ShowStmtType{
		ast.ShowBinlogStatus,
		ast.ShowEngines,
		ast.ShowDatabases,
		ast.ShowTables,
		ast.ShowTableStatus,
		ast.ShowColumns,
		ast.ShowWarnings,
		ast.ShowCharset,
		ast.ShowVariables,
		ast.ShowStatus,
		ast.ShowCollation,
		ast.ShowCreateTable,
		ast.ShowCreateUser,
		ast.ShowGrants,
		ast.ShowTriggers,
		ast.ShowProcedureStatus,
		ast.ShowIndex,
		ast.ShowProcessList,
		ast.ShowCreateDatabase,
		ast.ShowEvents,
		ast.ShowMasterStatus,
		ast.ShowBackups,
		ast.ShowRestores,
	}
	for _, tp := range tps {
		node.Tp = tp
		schema, _ := buildShowSchema(node, false, false)
		for _, col := range schema.Columns {
			require.Greater(t, col.RetType.GetFlen(), 0)
		}
	}
}

func TestGetPathByIndexName(t *testing.T) {
	tblInfo := &model.TableInfo{
		Indices:    make([]*model.IndexInfo, 0),
		PKIsHandle: true,
	}

	accessPath := []*util.AccessPath{
		{IsIntHandlePath: true},
		{Index: &model.IndexInfo{Name: ast.NewCIStr("idx")}},
		genTiFlashPath(tblInfo),
	}

	path := getPathByIndexName(accessPath, ast.NewCIStr("idx"), tblInfo)
	require.NotNil(t, path)
	require.Equal(t, accessPath[1], path)

	// "id" is a prefix of "idx"
	path = getPathByIndexName(accessPath, ast.NewCIStr("id"), tblInfo)
	require.NotNil(t, path)
	require.Equal(t, accessPath[1], path)

	path = getPathByIndexName(accessPath, ast.NewCIStr("primary"), tblInfo)
	require.NotNil(t, path)
	require.Equal(t, accessPath[0], path)

	path = getPathByIndexName(accessPath, ast.NewCIStr("not exists"), tblInfo)
	require.Nil(t, path)

	tblInfo = &model.TableInfo{
		Indices:    make([]*model.IndexInfo, 0),
		PKIsHandle: false,
	}

	path = getPathByIndexName(accessPath, ast.NewCIStr("primary"), tblInfo)
	require.Nil(t, path)
}

func TestRewriterPool(t *testing.T) {
	ctx := MockContext()
	defer func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	}()
	builder, _ := NewPlanBuilder().Init(ctx, nil, hint.NewQBHintHandler(nil))

	// Make sure PlanBuilder.getExpressionRewriter() provides clean rewriter from pool.
	// First, pick one rewriter from the pool and make it dirty.
	builder.rewriterCounter++
	dirtyRewriter := builder.getExpressionRewriter(context.TODO(), nil)
	dirtyRewriter.asScalar = true
	dirtyRewriter.planCtx.aggrMap = make(map[*ast.AggregateFuncExpr]int)
	dirtyRewriter.preprocess = func(ast.Node) ast.Node { return nil }
	dirtyRewriter.planCtx.insertPlan = &Insert{}
	dirtyRewriter.disableFoldCounter = 1
	dirtyRewriter.ctxStack = make([]expression.Expression, 2)
	dirtyRewriter.ctxNameStk = make([]*types.FieldName, 2)
	builder.rewriterCounter--
	// Then, pick again and check if it's cleaned up.
	builder.rewriterCounter++
	cleanRewriter := builder.getExpressionRewriter(context.TODO(), nil)
	require.Equal(t, dirtyRewriter, cleanRewriter)
	require.Equal(t, false, cleanRewriter.asScalar)
	require.Nil(t, cleanRewriter.planCtx.aggrMap)
	require.Nil(t, cleanRewriter.preprocess)
	require.Nil(t, cleanRewriter.planCtx.insertPlan)
	require.Zero(t, cleanRewriter.disableFoldCounter)
	require.Len(t, cleanRewriter.ctxStack, 0)
	builder.rewriterCounter--
}

func TestDisableFold(t *testing.T) {
	// Functions like BENCHMARK() shall not be folded into result 0,
	// but normal outer function with constant args should be folded.
	// Types of expression and first layer of args will be validated.
	cases := []struct {
		SQL      string
		Expected expression.Expression
		Args     []expression.Expression
	}{
		{`select sin(length("abc"))`, &expression.Constant{}, nil},
		{`select benchmark(3, sin(123))`, &expression.ScalarFunction{}, []expression.Expression{
			&expression.Constant{},
			&expression.ScalarFunction{},
		}},
		{`select pow(length("abc"), benchmark(3, sin(123)))`, &expression.ScalarFunction{}, []expression.Expression{
			&expression.Constant{},
			&expression.ScalarFunction{},
		}},
	}

	ctx := MockContext()
	defer func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	}()
	for _, c := range cases {
		st, err := parser.New().ParseOneStmt(c.SQL, "", "")
		require.NoError(t, err)
		stmt := st.(*ast.SelectStmt)
		expr := stmt.Fields.Fields[0].Expr

		builder, _ := NewPlanBuilder().Init(ctx, nil, hint.NewQBHintHandler(nil))
		builder.rewriterCounter++
		rewriter := builder.getExpressionRewriter(context.TODO(), nil)
		require.NotNil(t, rewriter)
		require.Equal(t, 0, rewriter.disableFoldCounter)
		rewrittenExpression, _, err := rewriteExprNode(rewriter, expr, true)
		require.NoError(t, err)
		require.Equal(t, 0, rewriter.disableFoldCounter)
		builder.rewriterCounter--

		require.IsType(t, c.Expected, rewrittenExpression)
		for i, expectedArg := range c.Args {
			rewrittenArg := expression.GetFuncArg(rewrittenExpression, i)
			require.IsType(t, expectedArg, rewrittenArg)
		}
	}
}

func TestDeepClone(t *testing.T) {
	tp := types.NewFieldType(mysql.TypeLonglong)
	expr := &expression.Column{RetType: tp}
	byItems := []*util.ByItems{{Expr: expr}}
	sort1 := &physicalop.PhysicalSort{ByItems: byItems}
	sort2 := &physicalop.PhysicalSort{ByItems: byItems}
	checkDeepClone := func(p1, p2 base.PhysicalPlan) error {
		whiteList := []string{"*property.StatsInfo", "*sessionctx.Context", "*mock.Context"}
		return checkDeepClonedCore(reflect.ValueOf(p1), reflect.ValueOf(p2), typeName(reflect.TypeOf(p1)), nil, whiteList, nil)
	}
	err := checkDeepClone(sort1, sort2)
	require.Error(t, err)
	require.Equal(t, "same slice pointers, path *PhysicalSort.ByItems", err.Error())

	byItems2 := []*util.ByItems{{Expr: expr}}
	sort2.ByItems = byItems2
	err = checkDeepClone(sort1, sort2)
	require.Error(t, err)
	require.Equal(t, "same pointer, path *PhysicalSort.ByItems[0].Expr", err.Error())

	expr2 := &expression.Column{RetType: tp}
	byItems2[0].Expr = expr2
	err = checkDeepClone(sort1, sort2)
	require.Error(t, err)
	require.Equal(t, "same pointer, path *PhysicalSort.ByItems[0].Expr.RetType", err.Error())

	expr2.RetType = types.NewFieldType(mysql.TypeString)
	err = checkDeepClone(sort1, sort2)
	require.Error(t, err)
	require.Equal(t, "different values, path *PhysicalSort.ByItems[0].Expr.RetType.tp", err.Error())

	expr2.RetType = types.NewFieldType(mysql.TypeLonglong)
	require.NoError(t, checkDeepClone(sort1, sort2))
}

func TestTablePlansAndTablePlanInPhysicalTableReaderClone(t *testing.T) {
	ctx := mock.NewContext()
	col, cst := &expression.Column{RetType: types.NewFieldType(mysql.TypeString)}, &expression.Constant{RetType: types.NewFieldType(mysql.TypeLonglong)}
	schema := expression.NewSchema(col)
	tblInfo := &model.TableInfo{}

	// table scan
	tableScan := &PhysicalTableScan{
		AccessCondition: []expression.Expression{col, cst},
		Table:           tblInfo,
	}
	tableScan = tableScan.Init(ctx, 0)
	tableScan.SetSchema(schema)

	// table reader
	tableReader := &PhysicalTableReader{
		tablePlan:  tableScan,
		TablePlans: []base.PhysicalPlan{tableScan},
		StoreType:  kv.TiFlash,
	}
	tableReader = tableReader.Init(ctx, 0)
	clonedPlan, err := tableReader.Clone(ctx)
	require.NoError(t, err)
	newTableReader, ok := clonedPlan.(*PhysicalTableReader)
	require.True(t, ok)
	require.True(t, newTableReader.tablePlan == newTableReader.TablePlans[0])
}

func TestPhysicalPlanClone(t *testing.T) {
	ctx := mock.NewContext()
	col, cst := &expression.Column{RetType: types.NewFieldType(mysql.TypeString)}, &expression.Constant{RetType: types.NewFieldType(mysql.TypeLonglong)}
	stats := &property.StatsInfo{RowCount: 1000}
	schema := expression.NewSchema(col)
	tblInfo := &model.TableInfo{}
	idxInfo := &model.IndexInfo{}
	aggDesc1, err := aggregation.NewAggFuncDesc(ctx, ast.AggFuncAvg, []expression.Expression{col}, false)
	require.NoError(t, err)
	aggDesc2, err := aggregation.NewAggFuncDesc(ctx, ast.AggFuncCount, []expression.Expression{cst}, true)
	require.NoError(t, err)
	aggDescs := []*aggregation.AggFuncDesc{aggDesc1, aggDesc2}

	// table scan
	tableScan := &PhysicalTableScan{
		AccessCondition: []expression.Expression{col, cst},
		Table:           tblInfo,
	}
	tableScan = tableScan.Init(ctx, 0)
	tableScan.SetSchema(schema)
	require.NoError(t, checkPhysicalPlanClone(tableScan))

	// table reader
	tableReader := &PhysicalTableReader{
		tablePlan:  tableScan,
		TablePlans: []base.PhysicalPlan{tableScan},
		StoreType:  kv.TiFlash,
	}
	tableReader = tableReader.Init(ctx, 0)
	require.NoError(t, checkPhysicalPlanClone(tableReader))

	// index scan
	indexScan := &PhysicalIndexScan{
		AccessCondition:  []expression.Expression{col, cst},
		Table:            tblInfo,
		Index:            idxInfo,
		dataSourceSchema: schema,
	}
	indexScan = indexScan.Init(ctx, 0)
	indexScan.SetSchema(schema)
	require.NoError(t, checkPhysicalPlanClone(indexScan))

	// index reader
	indexReader := &PhysicalIndexReader{
		indexPlan:     indexScan,
		IndexPlans:    []base.PhysicalPlan{indexScan},
		OutputColumns: []*expression.Column{col, col},
	}
	indexReader = indexReader.Init(ctx, 0)
	require.NoError(t, checkPhysicalPlanClone(indexReader))

	// index lookup
	indexLookup := &PhysicalIndexLookUpReader{
		IndexPlans:     []base.PhysicalPlan{indexReader},
		indexPlan:      indexScan,
		TablePlans:     []base.PhysicalPlan{tableReader},
		tablePlan:      tableScan,
		ExtraHandleCol: col,
		PushedLimit:    &PushedDownLimit{1, 2},
	}
	indexLookup = indexLookup.Init(ctx, 0)
	require.NoError(t, checkPhysicalPlanClone(indexLookup))

	// selection
	sel := &physicalop.PhysicalSelection{Conditions: []expression.Expression{col, cst}}
	sel = sel.Init(ctx, stats, 0)
	require.NoError(t, checkPhysicalPlanClone(sel))

	// maxOneRow
	maxOneRow := &PhysicalMaxOneRow{}
	maxOneRow = maxOneRow.Init(ctx, stats, 0)
	require.NoError(t, checkPhysicalPlanClone(maxOneRow))

	// projection
	proj := &PhysicalProjection{Exprs: []expression.Expression{col, cst}}
	proj = proj.Init(ctx, stats, 0)
	require.NoError(t, checkPhysicalPlanClone(proj))

	// limit
	lim := &physicalop.PhysicalLimit{Count: 1, Offset: 2}
	lim = lim.Init(ctx, stats, 0)
	require.NoError(t, checkPhysicalPlanClone(lim))

	// sort
	byItems := []*util.ByItems{{Expr: col}, {Expr: cst}}
	sort := &physicalop.PhysicalSort{ByItems: byItems}
	sort = sort.Init(ctx, stats, 0)
	require.NoError(t, checkPhysicalPlanClone(sort))

	// topN
	topN := &physicalop.PhysicalTopN{ByItems: byItems, Offset: 2333, Count: 2333}
	topN = topN.Init(ctx, stats, 0)
	require.NoError(t, checkPhysicalPlanClone(topN))

	// stream agg
	streamAgg := &PhysicalStreamAgg{basePhysicalAgg{
		AggFuncs:     aggDescs,
		GroupByItems: []expression.Expression{col, cst},
	}}
	streamAgg = streamAgg.initForStream(ctx, stats, 0)
	streamAgg.SetSchema(schema)
	require.NoError(t, checkPhysicalPlanClone(streamAgg))

	// hash agg
	hashAgg := &PhysicalHashAgg{
		basePhysicalAgg: basePhysicalAgg{
			AggFuncs:     aggDescs,
			GroupByItems: []expression.Expression{col, cst},
		},
	}
	hashAgg = hashAgg.initForHash(ctx, stats, 0)
	hashAgg.SetSchema(schema)
	require.NoError(t, checkPhysicalPlanClone(hashAgg))

	// hash join
	hashJoin := &PhysicalHashJoin{
		Concurrency:     4,
		UseOuterToBuild: true,
	}
	hashJoin = hashJoin.Init(ctx, stats, 0)
	hashJoin.SetSchema(schema)
	require.NoError(t, checkPhysicalPlanClone(hashJoin))

	// merge join
	mergeJoin := &PhysicalMergeJoin{
		CompareFuncs: []expression.CompareFunc{expression.CompareInt},
		Desc:         true,
	}
	mergeJoin = mergeJoin.Init(ctx, stats, 0)
	mergeJoin.SetSchema(schema)
	require.NoError(t, checkPhysicalPlanClone(mergeJoin))

	// index join
	baseJoin := basePhysicalJoin{
		LeftJoinKeys:    []*expression.Column{col},
		RightJoinKeys:   nil,
		OtherConditions: []expression.Expression{col},
	}

	indexJoin := &PhysicalIndexJoin{
		basePhysicalJoin: baseJoin,
		innerPlan:        indexScan,
		Ranges:           ranger.Ranges{},
	}
	indexJoin = indexJoin.Init(ctx, stats, 0)
	indexJoin.SetSchema(schema)
	require.NoError(t, checkPhysicalPlanClone(indexJoin))
}

//go:linkname valueInterface reflect.valueInterface
func valueInterface(v reflect.Value, safe bool) any

func typeName(t reflect.Type) string {
	path := t.String()
	tmp := strings.Split(path, ".")
	baseName := tmp[len(tmp)-1]
	if strings.HasPrefix(path, "*") { // is a pointer
		baseName = "*" + baseName
	}
	return baseName
}

func checkPhysicalPlanClone(p base.PhysicalPlan) error {
	cloned, err := p.Clone(p.SCtx())
	if err != nil {
		return err
	}
	whiteList := []string{"*property.StatsInfo", "*sessionctx.Context", "*mock.Context", "*types.FieldType"}
	return checkDeepClonedCore(reflect.ValueOf(p), reflect.ValueOf(cloned), typeName(reflect.TypeOf(p)), nil, whiteList, nil)
}

// checkDeepClonedCore is used to check if v2 is deep cloned from v1.
// It's modified from reflect.deepValueEqual. We cannot use reflect.DeepEqual here since they have different
// logic, for example, if two pointers point the same address, they will pass the DeepEqual check while failing in the DeepClone check.
func checkDeepClonedCore(v1, v2 reflect.Value, path string, whitePathList, whiteTypeList []string, visited map[visit]bool) error {
	skipPath := false
	for _, p := range whitePathList {
		if strings.HasSuffix(path, p) {
			skipPath = true
		}
	}
	if skipPath {
		return nil
	}

	if !v1.IsValid() || !v2.IsValid() {
		if v1.IsValid() != v2.IsValid() {
			return errors.Errorf("invalid")
		}
		return nil
	}
	if v1.Type() != v2.Type() {
		return errors.Errorf("different type %v, %v, path %v", v1.Type(), v2.Type(), path)
	}

	if visited == nil {
		visited = make(map[visit]bool)
	}
	hard := func(k reflect.Kind) bool {
		switch k {
		case reflect.Map, reflect.Slice, reflect.Ptr, reflect.Interface:
			return true
		}
		return false
	}
	if v1.CanAddr() && v2.CanAddr() && hard(v1.Kind()) {
		addr1 := unsafe.Pointer(v1.UnsafeAddr())
		addr2 := unsafe.Pointer(v2.UnsafeAddr())
		if uintptr(addr1) > uintptr(addr2) {
			addr1, addr2 = addr2, addr1
		}
		typ := v1.Type()
		v := visit{addr1, addr2, typ}
		if visited[v] {
			return nil
		}
		visited[v] = true
	}

	switch v1.Kind() {
	case reflect.Array:
		for i := range v1.Len() {
			if err := checkDeepClonedCore(v1.Index(i), v2.Index(i), fmt.Sprintf("%v[%v]", path, i), whitePathList, whiteTypeList, visited); err != nil {
				return err
			}
		}
	case reflect.Slice:
		if (v1.IsNil() && v2.IsNil()) || (v1.Len() == 0 && v2.Len() == 0) {
			return nil
		}
		if v1.Len() != v2.Len() {
			return errors.Errorf("different slice lengths, len %v, %v, path %v", v1.Len(), v2.Len(), path)
		}
		if v1.IsNil() != v2.IsNil() {
			if v1.Len() == 0 && v2.Len() == 0 {
				return nil // nil and an empty slice are accepted
			}
			return errors.Errorf("different slices nil %v, %v, path %v", v1.IsNil(), v2.IsNil(), path)
		}
		if v1.Pointer() == v2.Pointer() {
			return errors.Errorf("same slice pointers, path %v", path)
		}
		for i := range v1.Len() {
			if err := checkDeepClonedCore(v1.Index(i), v2.Index(i), fmt.Sprintf("%v[%v]", path, i), whitePathList, whiteTypeList, visited); err != nil {
				return err
			}
		}
	case reflect.Interface:
		if v1.IsNil() && v2.IsNil() {
			return nil
		}
		if v1.IsNil() != v2.IsNil() {
			return errors.Errorf("invalid interfaces, path %v", path)
		}
		return checkDeepClonedCore(v1.Elem(), v2.Elem(), path, whitePathList, whiteTypeList, visited)
	case reflect.Ptr:
		if v1.IsNil() && v2.IsNil() {
			return nil
		}
		if v1.Pointer() == v2.Pointer() {
			typeName := v1.Type().String()
			inWhiteList := slices.Contains(whiteTypeList, typeName)
			if inWhiteList {
				return nil
			}
			return errors.Errorf("same pointer, path %v", path)
		}
		return checkDeepClonedCore(v1.Elem(), v2.Elem(), path, whitePathList, whiteTypeList, visited)
	case reflect.Struct:
		for i, n := 0, v1.NumField(); i < n; i++ {
			fieldName := v1.Type().Field(i).Name
			if err := checkDeepClonedCore(v1.Field(i), v2.Field(i), fmt.Sprintf("%v.%v", path, fieldName), whitePathList, whiteTypeList, visited); err != nil {
				return err
			}
		}
	case reflect.Map:
		if v1.IsNil() && v2.IsNil() {
			return nil
		}
		if v1.IsNil() != v2.IsNil() || v1.Len() != v2.Len() {
			return errors.Errorf("different maps nil: %v, %v, len: %v, %v, path: %v", v1.IsNil(), v2.IsNil(), v1.Len(), v2.Len(), path)
		}
		if v1.Pointer() == v2.Pointer() {
			return errors.Errorf("same map pointers, path %v", path)
		}
		if len(v1.MapKeys()) != len(v2.MapKeys()) {
			return errors.Errorf("invalid map")
		}
		for _, k := range v1.MapKeys() {
			val1 := v1.MapIndex(k)
			val2 := v2.MapIndex(k)
			if !val1.IsValid() || !val2.IsValid() {
				return errors.Errorf("invalid map value at %v", fmt.Sprintf("%v[%v]", path, typeName(k.Type())))
			}
			if err := checkDeepClonedCore(val1, val2, fmt.Sprintf("%v[%v]", path, typeName(k.Type())), whitePathList, whiteTypeList, visited); err != nil {
				return err
			}
		}
	case reflect.Func:
		if v1.IsNil() != v2.IsNil() {
			return errors.Errorf("invalid functions, path %v", path)
		}
		return nil // assume that these functions are stateless
	default:
		if valueInterface(v1, false) != valueInterface(v2, false) {
			return errors.Errorf("different values, path %v", path)
		}
	}
	return nil
}

func TestHandleAnalyzeOptionsV1AndV2(t *testing.T) {
	require.Equal(t, len(analyzeOptionDefault), len(analyzeOptionDefaultV2), "analyzeOptionDefault and analyzeOptionDefaultV2 should have the same length")

	tests := []struct {
		name        string
		opts        []ast.AnalyzeOpt
		statsVer    int
		ExpectedErr string
	}{
		{
			name: "Too big TopN option",
			opts: []ast.AnalyzeOpt{
				{
					Type:  ast.AnalyzeOptNumTopN,
					Value: ast.NewValueExpr(100000+1, "", ""),
				},
			},
			statsVer:    statistics.Version1,
			ExpectedErr: "Value of analyze option TOPN should not be larger than 100000",
		},
		{
			name: "Use SampleRate option in stats version 1",
			opts: []ast.AnalyzeOpt{
				{
					Type:  ast.AnalyzeOptSampleRate,
					Value: ast.NewValueExpr(1, "", ""),
				},
			},
			statsVer:    statistics.Version1,
			ExpectedErr: "Version 1's statistics doesn't support the SAMPLERATE option, please set tidb_analyze_version to 2",
		},
		{
			name: "Too big SampleRate option",
			opts: []ast.AnalyzeOpt{
				{
					Type:  ast.AnalyzeOptSampleRate,
					Value: ast.NewValueExpr(2, "", ""),
				},
			},
			statsVer:    statistics.Version2,
			ExpectedErr: "Value of analyze option SAMPLERATE should not larger than 1.000000, and should be greater than 0",
		},
		{
			name: "Too big NumBuckets option",
			opts: []ast.AnalyzeOpt{
				{
					Type:  ast.AnalyzeOptNumBuckets,
					Value: ast.NewValueExpr(100000+1, "", ""),
				},
			},
			statsVer:    2,
			ExpectedErr: "Value of analyze option BUCKETS should be positive and not larger than 100000",
		},
		{
			name: "Set both sample num and sample rate",
			opts: []ast.AnalyzeOpt{
				{
					Type:  ast.AnalyzeOptNumSamples,
					Value: ast.NewValueExpr(100, "", ""),
				},
				{
					Type:  ast.AnalyzeOptSampleRate,
					Value: ast.NewValueExpr(0.1, "", ""),
				},
			},
			statsVer:    statistics.Version2,
			ExpectedErr: "ou can only either set the value of the sample num or set the value of the sample rate. Don't set both of them",
		},
		{
			name: "Too big CMSketchDepth and CMSketchWidth option",
			opts: []ast.AnalyzeOpt{
				{
					Type:  ast.AnalyzeOptCMSketchDepth,
					Value: ast.NewValueExpr(1024, "", ""),
				},
				{
					Type:  ast.AnalyzeOptCMSketchWidth,
					Value: ast.NewValueExpr(2048, "", ""),
				},
			},
			statsVer:    statistics.Version1,
			ExpectedErr: "cm sketch size(depth * width) should not larger than 1258291",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := handleAnalyzeOptions(tt.opts, tt.statsVer)
			if tt.ExpectedErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.ExpectedErr)
			} else {
				require.NoError(t, err)
			}

			if tt.statsVer == statistics.Version2 {
				_, err := handleAnalyzeOptionsV2(tt.opts)
				if tt.ExpectedErr != "" {
					require.Error(t, err)
					require.Contains(t, err.Error(), tt.ExpectedErr)
				} else {
					require.NoError(t, err)
				}
			}
		})
	}
}

func TestGetFullAnalyzeColumnsInfo(t *testing.T) {
	ctx := MockContext()
	defer func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	}()
	pb, _ := NewPlanBuilder().Init(ctx, nil, hint.NewQBHintHandler(nil))

	// Create a new TableName instance.
	tableName := &ast.TableName{
		Schema: ast.NewCIStr("test"),
		Name:   ast.NewCIStr("my_table"),
	}
	columns := []*model.ColumnInfo{
		{
			ID:        1,
			Name:      ast.NewCIStr("id"),
			FieldType: *types.NewFieldType(mysql.TypeLonglong),
		},
		{
			ID:        2,
			Name:      ast.NewCIStr("name"),
			FieldType: *types.NewFieldType(mysql.TypeString),
		},
		{
			ID:        3,
			Name:      ast.NewCIStr("age"),
			FieldType: *types.NewFieldType(mysql.TypeLonglong),
		},
	}
	tblNameW := &resolve.TableNameW{
		TableName: tableName,
		TableInfo: &model.TableInfo{
			Columns: columns,
		},
	}

	// Test case 1: AllColumns.
	cols, _, err := pb.getFullAnalyzeColumnsInfo(tblNameW, ast.AllColumns, nil, nil, nil, false, false)
	require.NoError(t, err)
	require.Equal(t, columns, cols)

	mustAnalyzedCols := &calcOnceMap{data: make(map[int64]struct{})}

	// TODO(0xPoe): Find a better way to mock SQL execution.
	// Test case 2: PredicateColumns(default)

	// Test case 3: ColumnList.
	specifiedCols := []*model.ColumnInfo{columns[0], columns[2]}
	mustAnalyzedCols.data[3] = struct{}{}
	cols, _, err = pb.getFullAnalyzeColumnsInfo(tblNameW, ast.ColumnList, specifiedCols, nil, mustAnalyzedCols, false, false)
	require.NoError(t, err)
	require.Equal(t, specifiedCols, cols)
}

func TestRequireInsertAndSelectPriv(t *testing.T) {
	ctx := MockContext()
	defer func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	}()
	pb, _ := NewPlanBuilder().Init(ctx, nil, hint.NewQBHintHandler(nil))

	tables := []*ast.TableName{
		{
			Schema: ast.NewCIStr("test"),
			Name:   ast.NewCIStr("t1"),
		},
		{
			Schema: ast.NewCIStr("test"),
			Name:   ast.NewCIStr("t2"),
		},
	}

	pb.requireInsertAndSelectPriv(tables)
	require.Len(t, pb.visitInfo, 4)
	require.Equal(t, "test", pb.visitInfo[0].db)
	require.Equal(t, "t1", pb.visitInfo[0].table)
	require.Equal(t, mysql.InsertPriv, pb.visitInfo[0].privilege)
	require.Equal(t, mysql.SelectPriv, pb.visitInfo[1].privilege)
}

func TestImportIntoCollAssignmentChecker(t *testing.T) {
	cases := []struct {
		expr       string
		error      string
		neededVars []string
	}{
		{
			expr:       "@a+1",
			neededVars: []string{"a"},
		},
		{
			expr:       "@b+@c+@1",
			neededVars: []string{"b", "c", "1"},
		},
		{
			expr: "instr(substr(concat_ws('','b','~~'), 6)) + sysdate()",
		},
		{
			expr: "now() + interval 1 day",
		},
		{
			expr: "sysdate() + interval 1 month",
		},
		{
			expr: "cast('123' as unsigned)",
		},
		{
			expr:       "getvar('c')",
			neededVars: []string{"c"},
		},
		{
			expr:  "a",
			error: "COLUMN reference is not supported in IMPORT INTO column assignment",
		},
		{
			expr:  "a+2",
			error: "COLUMN reference is not supported in IMPORT INTO column assignment",
		},
		{
			expr:  "(select 1)",
			error: "subquery is not supported in IMPORT INTO column assignment",
		},
		{
			expr:  "exists(select 1)",
			error: "subquery is not supported in IMPORT INTO column assignment",
		},
		{
			expr:  "1 in (select 1)",
			error: "subquery is not supported in IMPORT INTO column assignment",
		},
		{
			expr:  "1 + (select 1)",
			error: "subquery is not supported in IMPORT INTO column assignment",
		},
		{
			expr:  "@@sql_mode",
			error: "system variable is not supported in IMPORT INTO column assignment",
		},
		{
			expr:  "@@global.sql_mode",
			error: "system variable is not supported in IMPORT INTO column assignment",
		},
		{
			expr:  "@a:=1",
			error: "setting a variable in IMPORT INTO column assignment is not supported",
		},
		{
			expr:  "default(t.a)",
			error: "FUNCTION default is not supported in IMPORT INTO column assignment",
		},
		{
			expr:  "ROW_NUMBER() OVER(PARTITION BY 1)",
			error: "window FUNCTION ROW_NUMBER is not supported in IMPORT INTO column assignment",
		},
		{
			expr:  "COUNT(1)",
			error: "aggregate FUNCTION COUNT is not supported in IMPORT INTO column assignment",
		},
		{
			expr:  "grouping(1)",
			error: "FUNCTION grouping is not supported in IMPORT INTO column assignment",
		},
		{
			expr:  "getvar(concat('a', 'b'))",
			error: "the argument of getvar should be a constant string in IMPORT INTO column assignment",
		},
		{
			expr:  "getvar(now())",
			error: "the argument of getvar should be a constant string in IMPORT INTO column assignment",
		},
		{
			expr:  "noexist()",
			error: "FUNCTION noexist is not supported in IMPORT INTO column assignment",
		},
		{
			expr:  "values(a)",
			error: "COLUMN reference is not supported in IMPORT INTO column assignment",
		},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("case-%d-%s", i, c.expr), func(t *testing.T) {
			stmt, err := parser.New().ParseOneStmt("select "+c.expr, "", "")
			require.NoError(t, err, c.expr)
			expr := stmt.(*ast.SelectStmt).Fields.Fields[0].Expr

			checker := newImportIntoCollAssignmentChecker()
			checker.idx = i
			expr.Accept(checker)
			if c.error != "" {
				require.EqualError(t, checker.err, fmt.Sprintf("%s, index %d", c.error, i), c.expr)
			} else {
				require.NoError(t, checker.err, c.expr)
			}

			expectedNeededVars := make(map[string]int)
			for _, v := range c.neededVars {
				expectedNeededVars[v] = i
			}
			require.Equal(t, expectedNeededVars, checker.neededVars, c.expr)
		})
	}
}

func TestTraffic(t *testing.T) {
	tests := []struct {
		sql   string
		cols  int
		privs []string
	}{
		{
			sql:   "traffic capture to '/tmp' duration='1s' encryption_method='aes' compress=true",
			privs: []string{"TRAFFIC_CAPTURE_ADMIN"},
		},
		{
			sql:   "traffic replay from '/tmp' user='root' password='123456' speed=1.0 read_only=true",
			privs: []string{"TRAFFIC_REPLAY_ADMIN"},
		},
		{
			sql:   "show traffic jobs",
			privs: []string{"TRAFFIC_CAPTURE_ADMIN", "TRAFFIC_REPLAY_ADMIN"},
			cols:  8,
		},
		{
			sql:   "cancel traffic jobs",
			privs: []string{"TRAFFIC_CAPTURE_ADMIN", "TRAFFIC_REPLAY_ADMIN"},
		},
	}

	parser := parser.New()
	sctx := MockContext()
	ctx := context.TODO()
	for _, test := range tests {
		builder, _ := NewPlanBuilder().Init(sctx, nil, hint.NewQBHintHandler(nil))
		stmt, err := parser.ParseOneStmt(test.sql, "", "")
		require.NoError(t, err, test.sql)
		p, err := builder.Build(ctx, resolve.NewNodeW(stmt))
		require.NoError(t, err, test.sql)
		traffic, ok := p.(*Traffic)
		require.True(t, ok, test.sql)
		require.Equal(t, test.cols, len(traffic.names), test.sql)
		require.Equal(t, test.privs, builder.visitInfo[0].dynamicPrivs, test.sql)
	}
}

func TestBuildAdminAlterDDLJobPlan(t *testing.T) {
	parser := parser.New()
	sctx := MockContext()
	ctx := context.TODO()
	builder, _ := NewPlanBuilder().Init(sctx, nil, hint.NewQBHintHandler(nil))

	stmt, err := parser.ParseOneStmt("admin alter ddl jobs 1 thread = 16 ", "", "")
	require.NoError(t, err)
	p, err := builder.Build(ctx, resolve.NewNodeW(stmt))
	require.NoError(t, err)
	plan, ok := p.(*AlterDDLJob)
	require.True(t, ok)
	require.Equal(t, plan.JobID, int64(1))
	require.Len(t, plan.Options, 1)
	require.Equal(t, plan.Options[0].Name, AlterDDLJobThread)
	cons, ok := plan.Options[0].Value.(*expression.Constant)
	require.True(t, ok)
	require.Equal(t, cons.Value.GetInt64(), int64(16))

	stmt, err = parser.ParseOneStmt("admin alter ddl jobs 2 batch_size = 512 ", "", "")
	require.NoError(t, err)
	p, err = builder.Build(ctx, resolve.NewNodeW(stmt))
	require.NoError(t, err)
	plan, ok = p.(*AlterDDLJob)
	require.True(t, ok)
	require.Equal(t, plan.JobID, int64(2))
	require.Len(t, plan.Options, 1)
	require.Equal(t, plan.Options[0].Name, AlterDDLJobBatchSize)
	cons, ok = plan.Options[0].Value.(*expression.Constant)
	require.True(t, ok)
	require.Equal(t, cons.Value.GetInt64(), int64(512))

	stmt, err = parser.ParseOneStmt("admin alter ddl jobs 3 max_write_speed = '10MiB' ", "", "")
	require.NoError(t, err)
	p, err = builder.Build(ctx, resolve.NewNodeW(stmt))
	require.NoError(t, err)
	plan, ok = p.(*AlterDDLJob)
	require.True(t, ok)
	require.Equal(t, plan.JobID, int64(3))
	require.Len(t, plan.Options, 1)
	require.Equal(t, plan.Options[0].Name, AlterDDLJobMaxWriteSpeed)
	cons, ok = plan.Options[0].Value.(*expression.Constant)
	require.True(t, ok)
	require.Equal(t, cons.Value.GetString(), "10MiB")

	stmt, err = parser.ParseOneStmt("admin alter ddl jobs 4 max_write_speed = 1024", "", "")
	require.NoError(t, err)
	p, err = builder.Build(ctx, resolve.NewNodeW(stmt))
	require.NoError(t, err)
	plan, ok = p.(*AlterDDLJob)
	require.True(t, ok)
	require.Equal(t, plan.JobID, int64(4))
	require.Len(t, plan.Options, 1)
	require.Equal(t, AlterDDLJobMaxWriteSpeed, plan.Options[0].Name)
	cons, ok = plan.Options[0].Value.(*expression.Constant)
	require.True(t, ok)
	require.EqualValues(t, 1024, cons.Value.GetInt64())

	stmt, err = parser.ParseOneStmt("admin alter ddl jobs 5 thread = 16, batch_size = 512, max_write_speed = '10MiB' ", "", "")
	require.NoError(t, err)
	p, err = builder.Build(ctx, resolve.NewNodeW(stmt))
	require.NoError(t, err)
	plan, ok = p.(*AlterDDLJob)
	require.True(t, ok)
	require.Equal(t, plan.JobID, int64(5))
	require.Len(t, plan.Options, 3)
	sort.Slice(plan.Options, func(i, j int) bool {
		return plan.Options[i].Name < plan.Options[j].Name
	})
	require.Equal(t, plan.Options[0].Name, AlterDDLJobBatchSize)
	cons, ok = plan.Options[0].Value.(*expression.Constant)
	require.True(t, ok)
	require.Equal(t, cons.Value.GetInt64(), int64(512))
	require.Equal(t, plan.Options[1].Name, AlterDDLJobMaxWriteSpeed)
	cons, ok = plan.Options[1].Value.(*expression.Constant)
	require.True(t, ok)
	require.Equal(t, cons.Value.GetString(), "10MiB")
	require.Equal(t, plan.Options[2].Name, AlterDDLJobThread)
	cons, ok = plan.Options[2].Value.(*expression.Constant)
	require.True(t, ok)
	require.Equal(t, cons.Value.GetInt64(), int64(16))

	stmt, err = parser.ParseOneStmt("admin alter ddl jobs 4 aaa = 16", "", "")
	require.NoError(t, err)
	_, err = builder.Build(ctx, resolve.NewNodeW(stmt))
	require.Equal(t, err.Error(), "unsupported admin alter ddl jobs config: aaa")
}

func TestGetMaxWriteSpeedFromExpression(t *testing.T) {
	parser := parser.New()
	sctx := MockContext()
	ctx := context.TODO()
	builder, _ := NewPlanBuilder().Init(sctx, nil, hint.NewQBHintHandler(nil))
	// random speed value
	n := rand.Intn(units.PiB + 1)
	stmt, err := parser.ParseOneStmt(fmt.Sprintf("admin alter ddl jobs 1 max_write_speed = %d ", n), "", "")
	require.NoError(t, err)
	p, err := builder.Build(ctx, resolve.NewNodeW(stmt))
	require.NoError(t, err)
	plan, ok := p.(*AlterDDLJob)
	require.True(t, ok)
	require.Equal(t, plan.JobID, int64(1))
	require.Len(t, plan.Options, 1)
	require.Equal(t, plan.Options[0].Name, AlterDDLJobMaxWriteSpeed)
	_, ok = plan.Options[0].Value.(*expression.Constant)
	require.True(t, ok)
	maxWriteSpeed, err := GetMaxWriteSpeedFromExpression(plan.Options[0])
	require.NoError(t, err)
	require.Equal(t, int64(n), maxWriteSpeed)
	// parse speed string error
	opt := &AlterDDLJobOpt{
		Name:  "test",
		Value: expression.NewStrConst("MiB"),
	}
	_, err = GetMaxWriteSpeedFromExpression(opt)
	require.Equal(t, "parse max_write_speed value error: invalid size: 'MiB'", err.Error())
}

func TestProcessNextGenS3Path(t *testing.T) {
	u, err := url.Parse("S3://bucket?External-id=abc")
	require.NoError(t, err)
	_, err = processSemNextGenS3Path(u)
	require.ErrorIs(t, err, plannererrors.ErrNotSupportedWithSem)
	require.ErrorContains(t, err, "IMPORT INTO with S3 external ID")

	bak := config.GetGlobalKeyspaceName()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.KeyspaceName = "sem-next-gen"
	})
	t.Cleanup(func() {
		config.UpdateGlobal(func(conf *config.Config) {
			conf.KeyspaceName = bak
		})
	})
	u, err = url.Parse("s3://bucket")
	require.NoError(t, err)
	newPath, err := processSemNextGenS3Path(u)
	require.NoError(t, err)
	require.Equal(t, "s3://bucket?external-id=sem-next-gen", newPath)
}
