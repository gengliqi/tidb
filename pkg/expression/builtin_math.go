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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package expression

import (
	"fmt"
	"hash/crc32"
	"math"
	"strconv"
	"strings"

	perrors "github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &absFunctionClass{}
	_ functionClass = &roundFunctionClass{}
	_ functionClass = &ceilFunctionClass{}
	_ functionClass = &floorFunctionClass{}
	_ functionClass = &logFunctionClass{}
	_ functionClass = &log2FunctionClass{}
	_ functionClass = &log10FunctionClass{}
	_ functionClass = &randFunctionClass{}
	_ functionClass = &powFunctionClass{}
	_ functionClass = &convFunctionClass{}
	_ functionClass = &crc32FunctionClass{}
	_ functionClass = &signFunctionClass{}
	_ functionClass = &sqrtFunctionClass{}
	_ functionClass = &acosFunctionClass{}
	_ functionClass = &asinFunctionClass{}
	_ functionClass = &atanFunctionClass{}
	_ functionClass = &cosFunctionClass{}
	_ functionClass = &cotFunctionClass{}
	_ functionClass = &degreesFunctionClass{}
	_ functionClass = &expFunctionClass{}
	_ functionClass = &piFunctionClass{}
	_ functionClass = &radiansFunctionClass{}
	_ functionClass = &sinFunctionClass{}
	_ functionClass = &tanFunctionClass{}
	_ functionClass = &truncateFunctionClass{}
)

var (
	_ builtinFunc = &builtinAbsRealSig{}
	_ builtinFunc = &builtinAbsIntSig{}
	_ builtinFunc = &builtinAbsUIntSig{}
	_ builtinFunc = &builtinAbsDecSig{}
	_ builtinFunc = &builtinRoundRealSig{}
	_ builtinFunc = &builtinRoundIntSig{}
	_ builtinFunc = &builtinRoundDecSig{}
	_ builtinFunc = &builtinRoundWithFracRealSig{}
	_ builtinFunc = &builtinRoundWithFracIntSig{}
	_ builtinFunc = &builtinRoundWithFracDecSig{}
	_ builtinFunc = &builtinCeilRealSig{}
	_ builtinFunc = &builtinCeilIntToDecSig{}
	_ builtinFunc = &builtinCeilIntToIntSig{}
	_ builtinFunc = &builtinCeilDecToIntSig{}
	_ builtinFunc = &builtinCeilDecToDecSig{}
	_ builtinFunc = &builtinFloorRealSig{}
	_ builtinFunc = &builtinFloorIntToDecSig{}
	_ builtinFunc = &builtinFloorIntToIntSig{}
	_ builtinFunc = &builtinFloorDecToIntSig{}
	_ builtinFunc = &builtinFloorDecToDecSig{}
	_ builtinFunc = &builtinLog1ArgSig{}
	_ builtinFunc = &builtinLog2ArgsSig{}
	_ builtinFunc = &builtinLog2Sig{}
	_ builtinFunc = &builtinLog10Sig{}
	_ builtinFunc = &builtinRandSig{}
	_ builtinFunc = &builtinRandWithSeedFirstGenSig{}
	_ builtinFunc = &builtinPowSig{}
	_ builtinFunc = &builtinConvSig{}
	_ builtinFunc = &builtinCRC32Sig{}
	_ builtinFunc = &builtinSignSig{}
	_ builtinFunc = &builtinSqrtSig{}
	_ builtinFunc = &builtinAcosSig{}
	_ builtinFunc = &builtinAsinSig{}
	_ builtinFunc = &builtinAtan1ArgSig{}
	_ builtinFunc = &builtinAtan2ArgsSig{}
	_ builtinFunc = &builtinCosSig{}
	_ builtinFunc = &builtinCotSig{}
	_ builtinFunc = &builtinDegreesSig{}
	_ builtinFunc = &builtinExpSig{}
	_ builtinFunc = &builtinPISig{}
	_ builtinFunc = &builtinRadiansSig{}
	_ builtinFunc = &builtinSinSig{}
	_ builtinFunc = &builtinTanSig{}
	_ builtinFunc = &builtinTruncateIntSig{}
	_ builtinFunc = &builtinTruncateRealSig{}
	_ builtinFunc = &builtinTruncateDecimalSig{}
	_ builtinFunc = &builtinTruncateUintSig{}
)

type absFunctionClass struct {
	baseFunctionClass
}

func (c *absFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, c.verifyArgs(args)
	}

	argFieldTp := args[0].GetType(ctx.GetEvalCtx())
	argTp := argFieldTp.EvalType()
	if argTp != types.ETInt && argTp != types.ETDecimal {
		argTp = types.ETReal
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, argTp, argTp)
	if err != nil {
		return nil, err
	}
	if mysql.HasUnsignedFlag(argFieldTp.GetFlag()) {
		bf.tp.AddFlag(mysql.UnsignedFlag)
	}
	if argTp == types.ETReal {
		flen, decimal := mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeDouble)
		bf.tp.SetFlen(flen)
		bf.tp.SetDecimal(decimal)
	} else {
		bf.tp.SetFlenUnderLimit(argFieldTp.GetFlen())
		bf.tp.SetDecimalUnderLimit(argFieldTp.GetDecimal())
	}
	var sig builtinFunc
	switch argTp {
	case types.ETInt:
		if mysql.HasUnsignedFlag(argFieldTp.GetFlag()) {
			sig = &builtinAbsUIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_AbsUInt)
		} else {
			sig = &builtinAbsIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_AbsInt)
		}
	case types.ETDecimal:
		sig = &builtinAbsDecSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_AbsDecimal)
	case types.ETReal:
		sig = &builtinAbsRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_AbsReal)
	default:
		panic("unexpected argTp")
	}
	return sig, nil
}

type builtinAbsRealSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinAbsRealSig) Clone() builtinFunc {
	newSig := &builtinAbsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals ABS(value).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_abs
func (b *builtinAbsRealSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return math.Abs(val), false, nil
}

type builtinAbsIntSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinAbsIntSig) Clone() builtinFunc {
	newSig := &builtinAbsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals ABS(value).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_abs
func (b *builtinAbsIntSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val >= 0 {
		return val, false, nil
	}
	if val == math.MinInt64 {
		return 0, false, types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("abs(%d)", val))
	}
	return -val, false, nil
}

type builtinAbsUIntSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinAbsUIntSig) Clone() builtinFunc {
	newSig := &builtinAbsUIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals ABS(value).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_abs
func (b *builtinAbsUIntSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	return b.args[0].EvalInt(ctx, row)
}

type builtinAbsDecSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinAbsDecSig) Clone() builtinFunc {
	newSig := &builtinAbsDecSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals ABS(value).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_abs
func (b *builtinAbsDecSig) evalDecimal(ctx EvalContext, row chunk.Row) (*types.MyDecimal, bool, error) {
	val, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	to := new(types.MyDecimal)
	if !val.IsNegative() {
		*to = *val
	} else {
		if err = types.DecimalSub(new(types.MyDecimal), val, to); err != nil {
			return nil, true, err
		}
	}
	return to, false, nil
}

func (c *roundFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, c.verifyArgs(args)
	}
	argTp := args[0].GetType(ctx.GetEvalCtx()).EvalType()
	if argTp != types.ETInt && argTp != types.ETDecimal {
		argTp = types.ETReal
	}
	argTps := []types.EvalType{argTp}
	if len(args) > 1 {
		argTps = append(argTps, types.ETInt)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, argTp, argTps...)
	if err != nil {
		return nil, err
	}
	argFieldTp := args[0].GetType(ctx.GetEvalCtx())
	if mysql.HasUnsignedFlag(argFieldTp.GetFlag()) {
		bf.tp.AddFlag(mysql.UnsignedFlag)
	}

	// ETInt or ETReal is set correctly by newBaseBuiltinFuncWithTp, only need to handle ETDecimal.
	if argTp == types.ETDecimal {
		bf.tp.SetFlenUnderLimit(argFieldTp.GetFlen())
		bf.tp.SetDecimalUnderLimit(calculateDecimal4RoundAndTruncate(ctx, args, argTp))
		if bf.tp.GetDecimal() != types.UnspecifiedLength {
			if argFieldTp.GetDecimal() != types.UnspecifiedLength {
				decimalDelta := bf.tp.GetDecimal() - argFieldTp.GetDecimal()
				bf.tp.SetFlenUnderLimit(bf.tp.GetFlen() + max(decimalDelta, 0))
			} else {
				bf.tp.SetFlenUnderLimit(argFieldTp.GetFlen() + bf.tp.GetDecimal())
			}
		}
	}

	var sig builtinFunc
	if len(args) > 1 {
		switch argTp {
		case types.ETInt:
			sig = &builtinRoundWithFracIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_RoundWithFracInt)
		case types.ETDecimal:
			sig = &builtinRoundWithFracDecSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_RoundWithFracDec)
		case types.ETReal:
			sig = &builtinRoundWithFracRealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_RoundWithFracReal)
		default:
			panic("unexpected argTp")
		}
	} else {
		switch argTp {
		case types.ETInt:
			sig = &builtinRoundIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_RoundInt)
		case types.ETDecimal:
			sig = &builtinRoundDecSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_RoundDec)
		case types.ETReal:
			sig = &builtinRoundRealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_RoundReal)
		default:
			panic("unexpected argTp")
		}
	}
	return sig, nil
}

// calculateDecimal4RoundAndTruncate calculates tp.decimals of round/truncate func.
func calculateDecimal4RoundAndTruncate(ctx BuildContext, args []Expression, retType types.EvalType) int {
	if retType == types.ETInt || len(args) <= 1 {
		return 0
	}
	secondConst, secondIsConst := args[1].(*Constant)
	if !secondIsConst {
		return args[0].GetType(ctx.GetEvalCtx()).GetDecimal()
	}
	argDec, isNull, err := secondConst.EvalInt(ctx.GetEvalCtx(), chunk.Row{})
	if err != nil || isNull || argDec < 0 {
		return 0
	}
	if argDec > mysql.MaxDecimalScale {
		return mysql.MaxDecimalScale
	}
	return int(argDec)
}

type builtinRoundRealSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinRoundRealSig) Clone() builtinFunc {
	newSig := &builtinRoundRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals ROUND(value).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_round
func (b *builtinRoundRealSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return types.Round(val, 0), false, nil
}

type builtinRoundIntSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinRoundIntSig) Clone() builtinFunc {
	newSig := &builtinRoundIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals ROUND(value).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_round
func (b *builtinRoundIntSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	return b.args[0].EvalInt(ctx, row)
}

type builtinRoundDecSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinRoundDecSig) Clone() builtinFunc {
	newSig := &builtinRoundDecSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals ROUND(value).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_round
func (b *builtinRoundDecSig) evalDecimal(ctx EvalContext, row chunk.Row) (*types.MyDecimal, bool, error) {
	val, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	to := new(types.MyDecimal)
	if err = val.Round(to, 0, types.ModeHalfUp); err != nil {
		return nil, true, err
	}
	return to, false, nil
}

type builtinRoundWithFracRealSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinRoundWithFracRealSig) Clone() builtinFunc {
	newSig := &builtinRoundWithFracRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals ROUND(value, frac).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_round
func (b *builtinRoundWithFracRealSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	frac, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return types.Round(val, int(frac)), false, nil
}

type builtinRoundWithFracIntSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinRoundWithFracIntSig) Clone() builtinFunc {
	newSig := &builtinRoundWithFracIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals ROUND(value, frac).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_round
func (b *builtinRoundWithFracIntSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	frac, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return int64(types.Round(float64(val), int(frac))), false, nil
}

type builtinRoundWithFracDecSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinRoundWithFracDecSig) Clone() builtinFunc {
	newSig := &builtinRoundWithFracDecSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals ROUND(value, frac).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_round
func (b *builtinRoundWithFracDecSig) evalDecimal(ctx EvalContext, row chunk.Row) (*types.MyDecimal, bool, error) {
	val, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	frac, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	to := new(types.MyDecimal)
	if err = val.Round(to, min(int(frac), b.tp.GetDecimal()), types.ModeHalfUp); err != nil {
		return nil, true, err
	}
	return to, false, nil
}

type ceilFunctionClass struct {
	baseFunctionClass
}

func (c *ceilFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}

	retTp, argTp := getEvalTp4FloorAndCeil(ctx.GetEvalCtx(), args[0])
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, retTp, argTp)
	if err != nil {
		return nil, err
	}
	setFlag4FloorAndCeil(ctx.GetEvalCtx(), bf.tp, args[0])
	// ETInt or ETReal is set correctly by newBaseBuiltinFuncWithTp, only need to handle ETDecimal.
	if retTp == types.ETDecimal {
		bf.tp.SetFlenUnderLimit(args[0].GetType(ctx.GetEvalCtx()).GetFlen())
		bf.tp.SetDecimal(0)
	}

	switch argTp {
	case types.ETInt:
		if retTp == types.ETInt {
			sig = &builtinCeilIntToIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_CeilIntToInt)
		} else {
			sig = &builtinCeilIntToDecSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_CeilIntToDec)
		}
	case types.ETDecimal:
		if retTp == types.ETInt {
			sig = &builtinCeilDecToIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_CeilDecToInt)
		} else {
			sig = &builtinCeilDecToDecSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_CeilDecToDec)
		}
	default:
		sig = &builtinCeilRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CeilReal)
	}
	return sig, nil
}

type builtinCeilRealSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCeilRealSig) Clone() builtinFunc {
	newSig := &builtinCeilRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinCeilRealSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_ceil
func (b *builtinCeilRealSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return math.Ceil(val), false, nil
}

type builtinCeilIntToIntSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCeilIntToIntSig) Clone() builtinFunc {
	newSig := &builtinCeilIntToIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinCeilIntToIntSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_ceil
func (b *builtinCeilIntToIntSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	return b.args[0].EvalInt(ctx, row)
}

type builtinCeilIntToDecSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCeilIntToDecSig) Clone() builtinFunc {
	newSig := &builtinCeilIntToDecSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinCeilIntToDecSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_Ceil
func (b *builtinCeilIntToDecSig) evalDecimal(ctx EvalContext, row chunk.Row) (*types.MyDecimal, bool, error) {
	val, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return nil, true, err
	}

	if mysql.HasUnsignedFlag(b.args[0].GetType(ctx).GetFlag()) || val >= 0 {
		return types.NewDecFromUint(uint64(val)), false, nil
	}
	return types.NewDecFromInt(val), false, nil
}

type builtinCeilDecToIntSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCeilDecToIntSig) Clone() builtinFunc {
	newSig := &builtinCeilDecToIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinCeilDecToIntSig.
// Ceil receives
func (b *builtinCeilDecToIntSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	// err here will only be ErrOverFlow(will never happen) or ErrTruncate(can be ignored).
	res, err := val.ToInt()
	if err == types.ErrTruncated {
		err = nil
		if !val.IsNegative() {
			res = res + 1
		}
	}
	return res, false, err
}

type builtinCeilDecToDecSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCeilDecToDecSig) Clone() builtinFunc {
	newSig := &builtinCeilDecToDecSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinCeilDecToDecSig.
func (b *builtinCeilDecToDecSig) evalDecimal(ctx EvalContext, row chunk.Row) (*types.MyDecimal, bool, error) {
	val, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}

	res := new(types.MyDecimal)
	if val.IsNegative() {
		err = val.Round(res, 0, types.ModeTruncate)
		return res, err != nil, err
	}

	err = val.Round(res, 0, types.ModeTruncate)
	if err != nil || res.Compare(val) == 0 {
		return res, err != nil, err
	}

	err = types.DecimalAdd(res, types.NewDecFromInt(1), res)
	return res, err != nil, err
}

type floorFunctionClass struct {
	baseFunctionClass
}

// getEvalTp4FloorAndCeil gets the types.EvalType of FLOOR and CEIL.
func getEvalTp4FloorAndCeil(ctx EvalContext, arg Expression) (retTp, argTp types.EvalType) {
	fieldTp := arg.GetType(ctx)
	retTp, argTp = types.ETInt, fieldTp.EvalType()
	switch argTp {
	case types.ETInt:
		retTp = types.ETInt
	case types.ETDecimal:
		if fieldTp.GetFlen()-fieldTp.GetDecimal() > mysql.MaxIntWidth-2 { // len(math.MaxInt64) - 1
			retTp = types.ETDecimal
		}
	default:
		retTp, argTp = types.ETReal, types.ETReal
	}
	return retTp, argTp
}

// setFlag4FloorAndCeil sets return flag of FLOOR and CEIL.
func setFlag4FloorAndCeil(ctx EvalContext, tp *types.FieldType, arg Expression) {
	fieldTp := arg.GetType(ctx)
	if (fieldTp.GetType() == mysql.TypeLong || fieldTp.GetType() == mysql.TypeLonglong || fieldTp.GetType() == mysql.TypeNewDecimal) && mysql.HasUnsignedFlag(fieldTp.GetFlag()) {
		tp.AddFlag(mysql.UnsignedFlag)
	}
	// TODO: when argument type is timestamp, add not null flag.
}

func (c *floorFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}

	retTp, argTp := getEvalTp4FloorAndCeil(ctx.GetEvalCtx(), args[0])
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, retTp, argTp)
	if err != nil {
		return nil, err
	}
	setFlag4FloorAndCeil(ctx.GetEvalCtx(), bf.tp, args[0])

	// ETInt or ETReal is set correctly by newBaseBuiltinFuncWithTp, only need to handle ETDecimal.
	if retTp == types.ETDecimal {
		bf.tp.SetFlenUnderLimit(args[0].GetType(ctx.GetEvalCtx()).GetFlen())
		bf.tp.SetDecimal(0)
	}
	switch argTp {
	case types.ETInt:
		if retTp == types.ETInt {
			sig = &builtinFloorIntToIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_FloorIntToInt)
		} else {
			sig = &builtinFloorIntToDecSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_FloorIntToDec)
		}
	case types.ETDecimal:
		if retTp == types.ETInt {
			sig = &builtinFloorDecToIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_FloorDecToInt)
		} else {
			sig = &builtinFloorDecToDecSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_FloorDecToDec)
		}
	default:
		sig = &builtinFloorRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_FloorReal)
	}
	return sig, nil
}

type builtinFloorRealSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinFloorRealSig) Clone() builtinFunc {
	newSig := &builtinFloorRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinFloorRealSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_floor
func (b *builtinFloorRealSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return math.Floor(val), false, nil
}

type builtinFloorIntToIntSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinFloorIntToIntSig) Clone() builtinFunc {
	newSig := &builtinFloorIntToIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinFloorIntToIntSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_floor
func (b *builtinFloorIntToIntSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	return b.args[0].EvalInt(ctx, row)
}

type builtinFloorIntToDecSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinFloorIntToDecSig) Clone() builtinFunc {
	newSig := &builtinFloorIntToDecSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinFloorIntToDecSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_floor
func (b *builtinFloorIntToDecSig) evalDecimal(ctx EvalContext, row chunk.Row) (*types.MyDecimal, bool, error) {
	val, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return nil, true, err
	}

	if mysql.HasUnsignedFlag(b.args[0].GetType(ctx).GetFlag()) || val >= 0 {
		return types.NewDecFromUint(uint64(val)), false, nil
	}
	return types.NewDecFromInt(val), false, nil
}

type builtinFloorDecToIntSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinFloorDecToIntSig) Clone() builtinFunc {
	newSig := &builtinFloorDecToIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinFloorDecToIntSig.
// floor receives
func (b *builtinFloorDecToIntSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	// err here will only be ErrOverFlow(will never happen) or ErrTruncate(can be ignored).
	res, err := val.ToInt()
	if err == types.ErrTruncated {
		err = nil
		if val.IsNegative() {
			res--
		}
	}
	return res, false, err
}

type builtinFloorDecToDecSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinFloorDecToDecSig) Clone() builtinFunc {
	newSig := &builtinFloorDecToDecSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinFloorDecToDecSig.
func (b *builtinFloorDecToDecSig) evalDecimal(ctx EvalContext, row chunk.Row) (*types.MyDecimal, bool, error) {
	val, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return nil, true, err
	}

	res := new(types.MyDecimal)
	if !val.IsNegative() {
		err = val.Round(res, 0, types.ModeTruncate)
		return res, err != nil, err
	}

	err = val.Round(res, 0, types.ModeTruncate)
	if err != nil || res.Compare(val) == 0 {
		return res, err != nil, err
	}

	err = types.DecimalSub(res, types.NewDecFromInt(1), res)
	return res, err != nil, err
}

type logFunctionClass struct {
	baseFunctionClass
}

func (c *logFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	var (
		sig     builtinFunc
		bf      baseBuiltinFunc
		argsLen = len(args)
	)

	var err error
	if argsLen == 1 {
		bf, err = newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal)
		if err != nil {
			return nil, err
		}
	} else {
		bf, err = newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal, types.ETReal)
		if err != nil {
			return nil, err
		}
	}

	if argsLen == 1 {
		sig = &builtinLog1ArgSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Log1Arg)
	} else {
		sig = &builtinLog2ArgsSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Log2Args)
	}

	return sig, nil
}

type builtinLog1ArgSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLog1ArgSig) Clone() builtinFunc {
	newSig := &builtinLog1ArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinLog1ArgSig, corresponding to log(x).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_log
func (b *builtinLog1ArgSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val <= 0 {
		tc := typeCtx(ctx)
		tc.AppendWarning(ErrInvalidArgumentForLogarithm)
		return 0, true, nil
	}
	return math.Log(val), false, nil
}

type builtinLog2ArgsSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLog2ArgsSig) Clone() builtinFunc {
	newSig := &builtinLog2ArgsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinLog2ArgsSig, corresponding to log(b, x).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_log
func (b *builtinLog2ArgsSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val1, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	val2, isNull, err := b.args[1].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	if val1 <= 0 || val1 == 1 || val2 <= 0 {
		tc := typeCtx(ctx)
		tc.AppendWarning(ErrInvalidArgumentForLogarithm)
		return 0, true, nil
	}

	return math.Log(val2) / math.Log(val1), false, nil
}

type log2FunctionClass struct {
	baseFunctionClass
}

func (c *log2FunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal)
	if err != nil {
		return nil, err
	}
	sig := &builtinLog2Sig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Log2)
	return sig, nil
}

type builtinLog2Sig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLog2Sig) Clone() builtinFunc {
	newSig := &builtinLog2Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinLog2Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_log2
func (b *builtinLog2Sig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val <= 0 {
		tc := typeCtx(ctx)
		tc.AppendWarning(ErrInvalidArgumentForLogarithm)
		return 0, true, nil
	}
	return math.Log2(val), false, nil
}

type log10FunctionClass struct {
	baseFunctionClass
}

func (c *log10FunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal)
	if err != nil {
		return nil, err
	}
	sig := &builtinLog10Sig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Log10)
	return sig, nil
}

type builtinLog10Sig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLog10Sig) Clone() builtinFunc {
	newSig := &builtinLog10Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinLog10Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_log10
func (b *builtinLog10Sig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val <= 0 {
		tc := typeCtx(ctx)
		tc.AppendWarning(ErrInvalidArgumentForLogarithm)
		return 0, true, nil
	}
	return math.Log10(val), false, nil
}

type randFunctionClass struct {
	baseFunctionClass
}

func (c *randFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	var sig builtinFunc
	var argTps []types.EvalType
	if len(args) > 0 {
		argTps = []types.EvalType{types.ETInt}
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, argTps...)
	if err != nil {
		return nil, err
	}
	bt := bf
	if len(args) == 0 {
		sig = &builtinRandSig{bt, ctx.Rng()}
		sig.setPbCode(tipb.ScalarFuncSig_Rand)
	} else if _, isConstant := args[0].(*Constant); isConstant {
		// According to MySQL manual:
		// If an integer argument N is specified, it is used as the seed value:
		// With a constant initializer argument, the seed is initialized once
		// when the statement is prepared, prior to execution.
		seed, isNull, err := args[0].EvalInt(ctx.GetEvalCtx(), chunk.Row{})
		if err != nil {
			return nil, err
		}
		if isNull {
			// When the seed is null we need to use 0 as the seed.
			// The behavior same as MySQL.
			seed = 0
		}
		sig = &builtinRandSig{bt, mathutil.NewWithSeed(seed)}
		sig.setPbCode(tipb.ScalarFuncSig_Rand)
	} else {
		sig = &builtinRandWithSeedFirstGenSig{bt}
		sig.setPbCode(tipb.ScalarFuncSig_RandWithSeedFirstGen)
	}
	return sig, nil
}

type builtinRandSig struct {
	baseBuiltinFunc
	mysqlRng *mathutil.MysqlRng
}

func (b *builtinRandSig) Clone() builtinFunc {
	newSig := &builtinRandSig{mysqlRng: b.mysqlRng}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals RAND().
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_rand
func (b *builtinRandSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	res := b.mysqlRng.Gen()
	return res, false, nil
}

type builtinRandWithSeedFirstGenSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinRandWithSeedFirstGenSig) Clone() builtinFunc {
	newSig := &builtinRandWithSeedFirstGenSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals RAND(N).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_rand
func (b *builtinRandWithSeedFirstGenSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	seed, isNull, err := b.args[0].EvalInt(ctx, row)
	if err != nil {
		return 0, true, err
	}
	// b.args[0] is promised to be a non-constant(such as a column name) in
	// builtinRandWithSeedFirstGenSig, the seed is initialized with the value for each
	// invocation of RAND().
	var rng *mathutil.MysqlRng
	if !isNull {
		rng = mathutil.NewWithSeed(seed)
	} else {
		rng = mathutil.NewWithSeed(0)
	}
	return rng.Gen(), false, nil
}

type powFunctionClass struct {
	baseFunctionClass
}

func (c *powFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal, types.ETReal)
	if err != nil {
		return nil, err
	}
	sig := &builtinPowSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Pow)
	return sig, nil
}

type builtinPowSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinPowSig) Clone() builtinFunc {
	newSig := &builtinPowSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals POW(x, y).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_pow
func (b *builtinPowSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	x, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	y, isNull, err := b.args[1].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	power := math.Pow(x, y)
	if math.IsInf(power, -1) || math.IsInf(power, 1) || math.IsNaN(power) {
		return 0, false, types.ErrOverflow.GenWithStackByArgs("DOUBLE", fmt.Sprintf("pow(%s, %s)", strconv.FormatFloat(x, 'f', -1, 64), strconv.FormatFloat(y, 'f', -1, 64)))
	}
	return power, false, nil
}

type roundFunctionClass struct {
	baseFunctionClass
}

type convFunctionClass struct {
	baseFunctionClass
}

func (c *convFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETInt, types.ETInt)
	if err != nil {
		return nil, err
	}
	charset, collate := ctx.GetCharsetInfo()
	bf.tp.SetCharset(charset)
	bf.tp.SetCollate(collate)
	bf.tp.SetFlen(64)
	sig := &builtinConvSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Conv)
	return sig, nil
}

type builtinConvSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinConvSig) Clone() builtinFunc {
	newSig := &builtinConvSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals CONV(N,from_base,to_base).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_conv.
func (b *builtinConvSig) evalString(ctx EvalContext, row chunk.Row) (res string, isNull bool, err error) {
	var str string
	switch x := b.args[0].(type) {
	case *Constant:
		if x.Value.Kind() == types.KindBinaryLiteral {
			datum, err := x.Eval(ctx, row)
			if err != nil {
				return "", false, err
			}
			str = datum.GetBinaryLiteral().ToBitLiteralString(true)
		}
	case *ScalarFunction:
		if x.FuncName.L == ast.Cast {
			arg0 := x.GetArgs()[0]
			if arg0.GetType(ctx).Hybrid() || IsBinaryLiteral(arg0) {
				str, isNull, err = arg0.EvalString(ctx, row)
				if isNull || err != nil {
					return str, isNull, err
				}
				d := types.NewStringDatum(str)
				str = d.GetBinaryLiteral().ToBitLiteralString(true)
			}
		}
	}
	fromBase, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	toBase, isNull, err := b.args[2].EvalInt(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	if len(str) == 0 {
		str, isNull, err = b.args[0].EvalString(ctx, row)
		if isNull || err != nil {
			return res, isNull, err
		}
	} else {
		str, isNull, err = b.conv(str[2:], 2, fromBase)
		if err != nil {
			return str, isNull, err
		}
	}
	return b.conv(str, fromBase, toBase)
}
func (b *builtinConvSig) conv(str string, fromBase, toBase int64) (res string, isNull bool, err error) {
	var (
		signed     bool
		negative   bool
		ignoreSign bool
	)
	if fromBase < 0 {
		fromBase = -fromBase
		signed = true
	}

	if toBase < 0 {
		toBase = -toBase
		ignoreSign = true
	}

	if fromBase > 36 || fromBase < 2 || toBase > 36 || toBase < 2 {
		return res, true, nil
	}

	str = getValidPrefix(strings.TrimSpace(str), fromBase)
	if len(str) == 0 {
		return "0", false, nil
	}

	if str[0] == '-' {
		negative = true
		str = str[1:]
	}

	val, err := strconv.ParseUint(str, int(fromBase), 64)
	if err != nil {
		return res, false, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", str)
	}
	if signed {
		if negative && val > -math.MinInt64 {
			val = -math.MinInt64
		}
		if !negative && val > math.MaxInt64 {
			val = math.MaxInt64
		}
	}
	if negative {
		val = -val
	}

	if int64(val) < 0 {
		negative = true
	} else {
		negative = false
	}
	if ignoreSign && negative {
		val = 0 - val
	}

	s := strconv.FormatUint(val, int(toBase))
	if negative && ignoreSign {
		s = "-" + s
	}
	res = strings.ToUpper(s)
	return res, false, nil
}

type crc32FunctionClass struct {
	baseFunctionClass
}

func (c *crc32FunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(10)
	bf.tp.AddFlag(mysql.UnsignedFlag)
	sig := &builtinCRC32Sig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_CRC32)
	return sig, nil
}

type builtinCRC32Sig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCRC32Sig) Clone() builtinFunc {
	newSig := &builtinCRC32Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a CRC32(expr).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_crc32
func (b *builtinCRC32Sig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	x, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	r := crc32.ChecksumIEEE([]byte(x))
	return int64(r), false, nil
}

type signFunctionClass struct {
	baseFunctionClass
}

func (c *signFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETReal)
	if err != nil {
		return nil, err
	}
	sig := &builtinSignSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Sign)
	return sig, nil
}

type builtinSignSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinSignSig) Clone() builtinFunc {
	newSig := &builtinSignSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals SIGN(v).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_sign
func (b *builtinSignSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val > 0 {
		return 1, false, nil
	} else if val == 0 {
		return 0, false, nil
	}
	return -1, false, nil
}

type sqrtFunctionClass struct {
	baseFunctionClass
}

func (c *sqrtFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal)
	if err != nil {
		return nil, err
	}
	sig := &builtinSqrtSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Sqrt)
	return sig, nil
}

type builtinSqrtSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinSqrtSig) Clone() builtinFunc {
	newSig := &builtinSqrtSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a SQRT(x).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_sqrt
func (b *builtinSqrtSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val < 0 {
		return 0, true, nil
	}
	return math.Sqrt(val), false, nil
}

type acosFunctionClass struct {
	baseFunctionClass
}

func (c *acosFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal)
	if err != nil {
		return nil, err
	}
	sig := &builtinAcosSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Acos)
	return sig, nil
}

type builtinAcosSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinAcosSig) Clone() builtinFunc {
	newSig := &builtinAcosSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinAcosSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_acos
func (b *builtinAcosSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val < -1 || val > 1 {
		return 0, true, nil
	}

	return math.Acos(val), false, nil
}

type asinFunctionClass struct {
	baseFunctionClass
}

func (c *asinFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal)
	if err != nil {
		return nil, err
	}
	sig := &builtinAsinSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Asin)
	return sig, nil
}

type builtinAsinSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinAsinSig) Clone() builtinFunc {
	newSig := &builtinAsinSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinAsinSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_asin
func (b *builtinAsinSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	if val < -1 || val > 1 {
		return 0, true, nil
	}

	return math.Asin(val), false, nil
}

type atanFunctionClass struct {
	baseFunctionClass
}

func (c *atanFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	var (
		sig     builtinFunc
		bf      baseBuiltinFunc
		argsLen = len(args)
	)

	var err error
	if argsLen == 1 {
		bf, err = newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal)
		if err != nil {
			return nil, err
		}
	} else {
		bf, err = newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal, types.ETReal)
		if err != nil {
			return nil, err
		}
	}

	if argsLen == 1 {
		sig = &builtinAtan1ArgSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Atan1Arg)
	} else {
		sig = &builtinAtan2ArgsSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Atan2Args)
	}

	return sig, nil
}

type builtinAtan1ArgSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinAtan1ArgSig) Clone() builtinFunc {
	newSig := &builtinAtan1ArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinAtan1ArgSig, corresponding to atan(x).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_atan
func (b *builtinAtan1ArgSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	return math.Atan(val), false, nil
}

type builtinAtan2ArgsSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinAtan2ArgsSig) Clone() builtinFunc {
	newSig := &builtinAtan2ArgsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinAtan1ArgSig, corresponding to atan(y, x).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_atan
func (b *builtinAtan2ArgsSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val1, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	val2, isNull, err := b.args[1].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	return math.Atan2(val1, val2), false, nil
}

type cosFunctionClass struct {
	baseFunctionClass
}

func (c *cosFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal)
	if err != nil {
		return nil, err
	}
	sig := &builtinCosSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Cos)
	return sig, nil
}

type builtinCosSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCosSig) Clone() builtinFunc {
	newSig := &builtinCosSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinCosSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_cos
func (b *builtinCosSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return math.Cos(val), false, nil
}

type cotFunctionClass struct {
	baseFunctionClass
}

func (c *cotFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal)
	if err != nil {
		return nil, err
	}
	sig := &builtinCotSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Cot)
	return sig, nil
}

type builtinCotSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCotSig) Clone() builtinFunc {
	newSig := &builtinCotSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinCotSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_cot
func (b *builtinCotSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	tan := math.Tan(val)
	if tan != 0 {
		cot := 1 / tan
		if !math.IsInf(cot, 0) && !math.IsNaN(cot) {
			return cot, false, nil
		}
	}
	return 0, false, types.ErrOverflow.GenWithStackByArgs("DOUBLE", fmt.Sprintf("cot(%s)", strconv.FormatFloat(val, 'f', -1, 64)))
}

type degreesFunctionClass struct {
	baseFunctionClass
}

func (c *degreesFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal)
	if err != nil {
		return nil, err
	}
	sig := &builtinDegreesSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Degrees)
	return sig, nil
}

type builtinDegreesSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinDegreesSig) Clone() builtinFunc {
	newSig := &builtinDegreesSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinDegreesSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_degrees
func (b *builtinDegreesSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	res := val * 180 / math.Pi
	return res, false, nil
}

type expFunctionClass struct {
	baseFunctionClass
}

func (c *expFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal)
	if err != nil {
		return nil, err
	}
	sig := &builtinExpSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Exp)
	return sig, nil
}

type builtinExpSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinExpSig) Clone() builtinFunc {
	newSig := &builtinExpSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinExpSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_exp
func (b *builtinExpSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	exp := math.Exp(val)
	if math.IsInf(exp, 0) || math.IsNaN(exp) {
		s := fmt.Sprintf("exp(%s)", b.args[0].StringWithCtx(ctx, perrors.RedactLogDisable))
		return 0, false, types.ErrOverflow.GenWithStackByArgs("DOUBLE", s)
	}
	return exp, false, nil
}

type piFunctionClass struct {
	baseFunctionClass
}

func (c *piFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	var (
		bf  baseBuiltinFunc
		sig builtinFunc
	)

	var err error
	bf, err = newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal)
	if err != nil {
		return nil, err
	}
	bf.tp.SetDecimal(6)
	bf.tp.SetFlen(8)
	sig = &builtinPISig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_PI)
	return sig, nil
}

type builtinPISig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinPISig) Clone() builtinFunc {
	newSig := &builtinPISig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinPISig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_pi
func (b *builtinPISig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	return float64(math.Pi), false, nil
}

type radiansFunctionClass struct {
	baseFunctionClass
}

func (c *radiansFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal)
	if err != nil {
		return nil, err
	}
	sig := &builtinRadiansSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Radians)
	return sig, nil
}

type builtinRadiansSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinRadiansSig) Clone() builtinFunc {
	newSig := &builtinRadiansSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals RADIANS(X).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_radians
func (b *builtinRadiansSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	x, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return x * (math.Pi / 180), false, nil
}

type sinFunctionClass struct {
	baseFunctionClass
}

func (c *sinFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal)
	if err != nil {
		return nil, err
	}
	sig := &builtinSinSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Sin)
	return sig, nil
}

type builtinSinSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinSinSig) Clone() builtinFunc {
	newSig := &builtinSinSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinSinSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_sin
func (b *builtinSinSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return math.Sin(val), false, nil
}

type tanFunctionClass struct {
	baseFunctionClass
}

func (c *tanFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal)
	if err != nil {
		return nil, err
	}
	sig := &builtinTanSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Tan)
	return sig, nil
}

type builtinTanSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinTanSig) Clone() builtinFunc {
	newSig := &builtinTanSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinTanSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_tan
func (b *builtinTanSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return math.Tan(val), false, nil
}

type truncateFunctionClass struct {
	baseFunctionClass
}

func (c *truncateFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	argTp := args[0].GetType(ctx.GetEvalCtx()).EvalType()
	if argTp.IsStringKind() {
		argTp = types.ETReal
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, argTp, argTp, types.ETInt)
	if err != nil {
		return nil, err
	}
	// ETInt or ETReal is set correctly by newBaseBuiltinFuncWithTp, only need to handle ETDecimal.
	if argTp == types.ETDecimal {
		bf.tp.SetDecimalUnderLimit(calculateDecimal4RoundAndTruncate(ctx, args, argTp))
		bf.tp.SetFlenUnderLimit(args[0].GetType(ctx.GetEvalCtx()).GetFlen() - args[0].GetType(ctx.GetEvalCtx()).GetDecimal() + bf.tp.GetDecimal())
	}
	argFieldTp := args[0].GetType(ctx.GetEvalCtx())
	if mysql.HasUnsignedFlag(argFieldTp.GetFlag()) {
		bf.tp.AddFlag(mysql.UnsignedFlag)
	}

	var sig builtinFunc
	switch argTp {
	case types.ETInt:
		if mysql.HasUnsignedFlag(args[0].GetType(ctx.GetEvalCtx()).GetFlag()) {
			sig = &builtinTruncateUintSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_TruncateUint)
		} else {
			sig = &builtinTruncateIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_TruncateInt)
		}
	case types.ETReal:
		sig = &builtinTruncateRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_TruncateReal)
	case types.ETDecimal:
		sig = &builtinTruncateDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_TruncateDecimal)
	default:
		return nil, errIncorrectArgs.GenWithStackByArgs("truncate")
	}

	return sig, nil
}

type builtinTruncateDecimalSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinTruncateDecimalSig) Clone() builtinFunc {
	newSig := &builtinTruncateDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals a TRUNCATE(X,D).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_truncate
func (b *builtinTruncateDecimalSig) evalDecimal(ctx EvalContext, row chunk.Row) (*types.MyDecimal, bool, error) {
	x, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}

	d, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}

	result := new(types.MyDecimal)
	if err := x.Round(result, min(int(d), b.getRetTp().GetDecimal()), types.ModeTruncate); err != nil {
		return nil, true, err
	}
	return result, false, nil
}

type builtinTruncateRealSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinTruncateRealSig) Clone() builtinFunc {
	newSig := &builtinTruncateRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a TRUNCATE(X,D).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_truncate
func (b *builtinTruncateRealSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	x, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	d, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	return types.Truncate(x, int(d)), false, nil
}

type builtinTruncateIntSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinTruncateIntSig) Clone() builtinFunc {
	newSig := &builtinTruncateIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a TRUNCATE(X,D).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_truncate
func (b *builtinTruncateIntSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	x, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	d, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	if mysql.HasUnsignedFlag(b.args[1].GetType(ctx).GetFlag()) {
		return x, false, nil
	}

	if d >= 0 {
		return x, false, nil
	}
	// -MinInt = MinInt, special case
	if d == mathutil.MinInt {
		return 0, false, nil
	}
	shift := int64(math.Pow10(int(-d)))
	return x / shift * shift, false, nil
}

func (b *builtinTruncateUintSig) Clone() builtinFunc {
	newSig := &builtinTruncateUintSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

type builtinTruncateUintSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

// evalInt evals a TRUNCATE(X,D).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_truncate
func (b *builtinTruncateUintSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	x, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if mysql.HasUnsignedFlag(b.args[1].GetType(ctx).GetFlag()) {
		return x, false, nil
	}
	uintx := uint64(x)

	d, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if d >= 0 {
		return x, false, nil
	}
	// -MinInt = MinInt, special case
	if d == mathutil.MinInt {
		return 0, false, nil
	}
	shift := uint64(math.Pow10(int(-d)))
	return int64(uintx / shift * shift), false, nil
}
