// Copyright 2019 PingCAP, Inc.
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

package ddl

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	tikverr "github.com/tikv/client-go/v2/error"
	"go.uber.org/zap"
)

func splitPartitionTableRegion(ctx sessionctx.Context, store kv.SplittableStore, tbInfo *model.TableInfo, parts []model.PartitionDefinition, scatterScope string) {
	// Max partition count is 8192, should we sample and just choose some partitions to split?
	regionIDs := make([]uint64, 0, len(parts))
	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), ctx.GetSessionVars().GetSplitRegionTimeout())
	defer cancel()
	ctxWithTimeout = kv.WithInternalSourceType(ctxWithTimeout, kv.InternalTxnDDL)
	if shardingBits(tbInfo) > 0 && tbInfo.PreSplitRegions > 0 {
		for _, def := range parts {
			regionIDs = append(regionIDs, preSplitPhysicalTableByShardRowID(ctxWithTimeout, store, tbInfo, def.ID, scatterScope)...)
		}
	} else {
		for _, def := range parts {
			regionIDs = append(regionIDs, SplitRecordRegion(ctxWithTimeout, store, def.ID, tbInfo.ID, scatterScope))
		}
	}
	if scatterScope != vardef.ScatterOff {
		WaitScatterRegionFinish(ctxWithTimeout, store, regionIDs...)
	}
}

func splitTableRegion(ctx sessionctx.Context, store kv.SplittableStore, tbInfo *model.TableInfo, scatterScope string) {
	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), ctx.GetSessionVars().GetSplitRegionTimeout())
	defer cancel()
	ctxWithTimeout = kv.WithInternalSourceType(ctxWithTimeout, kv.InternalTxnDDL)
	var regionIDs []uint64
	if shardingBits(tbInfo) > 0 && tbInfo.PreSplitRegions > 0 {
		regionIDs = preSplitPhysicalTableByShardRowID(ctxWithTimeout, store, tbInfo, tbInfo.ID, scatterScope)
	} else {
		regionIDs = append(regionIDs, SplitRecordRegion(ctxWithTimeout, store, tbInfo.ID, tbInfo.ID, scatterScope))
	}
	if scatterScope != vardef.ScatterOff {
		WaitScatterRegionFinish(ctxWithTimeout, store, regionIDs...)
	}
}

// `tID` is used to control the scope of scatter. If it is `ScatterTable`, the corresponding tableID is used.
// If it is `ScatterGlobal`, the scatter configured at global level uniformly use -1 as `tID`.
func getScatterConfig(scope string, tableID int64) (scatter bool, tID int64) {
	switch scope {
	case vardef.ScatterTable:
		return true, tableID
	case vardef.ScatterGlobal:
		return true, -1
	default:
		return false, tableID
	}
}

func preSplitPhysicalTableByShardRowID(ctx context.Context, store kv.SplittableStore, tbInfo *model.TableInfo, physicalID int64, scatterScope string) []uint64 {
	// Example:
	// sharding_bits = 4
	// PreSplitRegions = 2
	//
	// then will pre-split 2^2 = 4 regions.
	//
	// in this code:
	// max   = 1 << sharding_bits = 16
	// step := int64(1 << (sharding_bits - tblInfo.PreSplitRegions)) = 1 << (4-2) = 4;
	//
	// then split regionID is below:
	// 4  << 59 = 2305843009213693952
	// 8  << 59 = 4611686018427387904
	// 12 << 59 = 6917529027641081856
	//
	// The 4 pre-split regions range is below:
	// 0                   ~ 2305843009213693952
	// 2305843009213693952 ~ 4611686018427387904
	// 4611686018427387904 ~ 6917529027641081856
	// 6917529027641081856 ~ 9223372036854775807 ( (1 << 63) - 1 )
	//
	// And the max _tidb_rowid is 9223372036854775807, it won't be negative number.

	// Split table region.
	var ft *types.FieldType
	if pkCol := tbInfo.GetPkColInfo(); pkCol != nil {
		ft = &pkCol.FieldType
	} else {
		ft = types.NewFieldType(mysql.TypeLonglong)
	}
	shardFmt := autoid.NewShardIDFormat(ft, shardingBits(tbInfo), tbInfo.AutoRandomRangeBits)
	step := int64(1 << (shardFmt.ShardBits - tbInfo.PreSplitRegions))
	maxv := int64(1 << shardFmt.ShardBits)
	splitTableKeys := make([][]byte, 0, 1<<(tbInfo.PreSplitRegions))
	splitTableKeys = append(splitTableKeys, tablecodec.GenTablePrefix(physicalID))
	for p := step; p < maxv; p += step {
		recordID := p << shardFmt.IncrementalBits
		recordPrefix := tablecodec.GenTableRecordPrefix(physicalID)
		key := tablecodec.EncodeRecordKey(recordPrefix, kv.IntHandle(recordID))
		splitTableKeys = append(splitTableKeys, key)
	}
	scatter, tableID := getScatterConfig(scatterScope, tbInfo.ID)
	regionIDs, err := store.SplitRegions(ctx, splitTableKeys, scatter, &tableID)
	if err != nil {
		logutil.DDLLogger().Warn("pre split some table regions failed",
			zap.Stringer("table", tbInfo.Name), zap.Int("successful region count", len(regionIDs)), zap.Error(err))
	}
	regionIDs = append(regionIDs, splitIndexRegion(store, tbInfo, scatter, &tableID)...)
	return regionIDs
}

// SplitRecordRegion is to split region in store by table prefix.
func SplitRecordRegion(ctx context.Context, store kv.SplittableStore, physicalTableID, tableID int64, scatterScope string) uint64 {
	tableStartKey := tablecodec.GenTablePrefix(physicalTableID)
	scatter, tID := getScatterConfig(scatterScope, tableID)
	regionIDs, err := store.SplitRegions(ctx, [][]byte{tableStartKey}, scatter, &tID)
	if err != nil {
		// It will be automatically split by TiKV later.
		logutil.DDLLogger().Warn("split table region failed", zap.Error(err))
	}
	if len(regionIDs) == 1 {
		return regionIDs[0]
	}
	return 0
}

func splitIndexRegion(store kv.SplittableStore, tblInfo *model.TableInfo, scatter bool, tableID *int64) []uint64 {
	splitKeys := make([][]byte, 0, len(tblInfo.Indices))
	for _, idx := range tblInfo.Indices {
		indexPrefix := tablecodec.EncodeTableIndexPrefix(tblInfo.ID, idx.ID)
		splitKeys = append(splitKeys, indexPrefix)
	}
	regionIDs, err := store.SplitRegions(context.Background(), splitKeys, scatter, tableID)
	if err != nil {
		logutil.DDLLogger().Warn("pre split some table index regions failed",
			zap.Stringer("table", tblInfo.Name), zap.Int("successful region count", len(regionIDs)), zap.Error(err))
	}
	return regionIDs
}

// WaitScatterRegionFinish will block until all regions are scattered.
func WaitScatterRegionFinish(ctx context.Context, store kv.SplittableStore, regionIDs ...uint64) {
	for _, regionID := range regionIDs {
		err := store.WaitScatterRegionFinish(ctx, regionID, 0)
		if err != nil {
			logutil.DDLLogger().Warn("wait scatter region failed", zap.Uint64("regionID", regionID), zap.Error(err))
			// We don't break for PDError because it may caused by ScatterRegion request failed.
			if _, ok := errors.Cause(err).(*tikverr.PDError); !ok {
				break
			}
		}
	}
}
