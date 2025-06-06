// Copyright 2023 PingCAP, Inc.
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

package storage

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/cache"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	handle_metrics "github.com/pingcap/tidb/pkg/statistics/handle/metrics"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/statistics/handle/usage/predicatecolumn"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	statsutil "github.com/pingcap/tidb/pkg/statistics/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

// statsReadWriter implements the util.StatsReadWriter interface.
type statsReadWriter struct {
	statsHandler statstypes.StatsHandle
}

// NewStatsReadWriter creates a new StatsReadWriter.
func NewStatsReadWriter(statsHandler statstypes.StatsHandle) statstypes.StatsReadWriter {
	return &statsReadWriter{statsHandler: statsHandler}
}

// ChangeGlobalStatsID changes the table ID in global-stats to the new table ID.
func (s *statsReadWriter) ChangeGlobalStatsID(from, to int64) (err error) {
	return util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		return errors.Trace(ChangeGlobalStatsID(util.StatsCtx, sctx, from, to))
	}, util.FlagWrapTxn)
}

// UpdateStatsMetaVersionForGC update the version of mysql.stats_meta. See more
// details in the interface definition.
func (s *statsReadWriter) UpdateStatsMetaVersionForGC(physicalID int64) (err error) {
	statsVer := uint64(0)
	defer func() {
		if err == nil && statsVer != 0 {
			s.statsHandler.RecordHistoricalStatsMeta(statsVer, util.StatsMetaHistorySourceSchemaChange, false, physicalID)
		}
	}()

	return util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		startTS, err := UpdateStatsMetaVerAndLastHistUpdateVer(util.StatsCtx, sctx, physicalID)
		if err != nil {
			return errors.Trace(err)
		}
		statsVer = startTS
		return nil
	}, util.FlagWrapTxn)
}

// handleSlowStatsSaving is used to handle the slow stats saving process.
// Once the saving process is too slow, we need to log a warning.
// Also, we need to update the stats meta with a more recent version to avoid other nodes missing the delta update.
// Combined with the stats meta version update, we can ensure the stats cache on other TiDB nodes is consistent.
// See more at stats cache's Update function.
func (s *statsReadWriter) handleSlowStatsSaving(tableID int64, start time.Time) (uint64, error) {
	dur := time.Since(start)
	// Note: In unit tests, the lease is set to a value less than 0, which means the lease is disabled.
	// This is why we need to explicitly check the lease here. Without this check,
	// the duration validation would always evaluate to true, which is not the intended behavior.
	isLoadIntervalExceeded := s.statsHandler.Lease() > 0 && dur >= cache.LeaseOffset*s.statsHandler.Lease()
	// Use failpoint to simulate slow saving.
	failpoint.Inject("slowStatsSaving", func(val failpoint.Value) {
		if val.(bool) {
			isLoadIntervalExceeded = true
		}
	})

	if !isLoadIntervalExceeded {
		return 0, nil
	}

	statslogutil.StatsLogger().Warn("Update stats cache is too slow",
		zap.Duration("duration", dur),
		zap.Int64("physicalID", tableID),
	)

	// Update stats meta to avoid other nodes missing the delta update.
	statsVer := uint64(0)
	err := util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		startTS, err := UpdateStatsMetaVerAndLastHistUpdateVer(util.StatsCtx, sctx, tableID)
		failpoint.Inject("failToSaveStats", func(val failpoint.Value) {
			if val.(bool) {
				err = errors.New("mock update stats meta version failed")
			}
		})
		if err != nil {
			return errors.Trace(err)
		}
		statsVer = startTS
		return nil
	}, util.FlagWrapTxn)
	if err != nil {
		statslogutil.StatsLogger().Error("Failed to update stats meta version for slow saving, the stats cache on other TiDB nodes may be inconsistent",
			zap.Int64("physicalID", tableID),
			zap.Error(err),
		)
		return 0, errors.Errorf("failed to update stats meta version during analyze result save. The system may be too busy. Please retry the operation later")
	}

	statslogutil.StatsLogger().Info("Successfully updated stats meta version for slow saving",
		zap.Uint64("statsVer", statsVer),
		zap.Int64("physicalID", tableID),
	)
	return statsVer, nil
}

// SaveAnalyzeResultToStorage saves the stats of a table to storage.
func (s *statsReadWriter) SaveAnalyzeResultToStorage(results *statistics.AnalyzeResults, analyzeSnapshot bool, source string) (err error) {
	var statsVer uint64
	start := time.Now()
	err = util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		statsVer, err = SaveAnalyzeResultToStorage(sctx, results, analyzeSnapshot)
		return err
	}, util.FlagWrapTxn)
	if err == nil && statsVer != 0 {
		tableID := results.TableID.GetStatisticsID()
		// Check if saving was slow and update stats version if needed
		version, err2 := s.handleSlowStatsSaving(tableID, start)
		if err2 != nil {
			statslogutil.StatsLogger().Error("Failed to update stats meta version for slow saving during analyze job execution",
				zap.Int64("physicalID", tableID),
				zap.String("database", results.Job.DBName),
				zap.String("table", results.Job.TableName),
				zap.String("partition", results.Job.PartitionName),
				zap.String("jobInfo", results.Job.JobInfo),
				zap.Error(err2),
			)
			return err2
		}
		if version != 0 {
			statsVer = version
		}
		s.statsHandler.RecordHistoricalStatsMeta(statsVer, source, true, tableID)
	}
	return err
}

// StatsMetaCountAndModifyCount reads count and modify_count for the given table from mysql.stats_meta.
func (s *statsReadWriter) StatsMetaCountAndModifyCount(tableID int64) (count, modifyCount int64, err error) {
	err = util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		count, modifyCount, _, err = StatsMetaCountAndModifyCount(util.StatsCtx, sctx, tableID)
		return err
	}, util.FlagWrapTxn)
	return
}

// TableStatsFromStorage loads table stats info from storage.
func (s *statsReadWriter) TableStatsFromStorage(tableInfo *model.TableInfo, physicalID int64, loadAll bool, snapshot uint64) (statsTbl *statistics.Table, err error) {
	err = util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		var ok bool
		statsTbl, ok = s.statsHandler.Get(physicalID)
		if !ok {
			statsTbl = nil
		}
		statsTbl, err = TableStatsFromStorage(sctx, snapshot, tableInfo, physicalID, loadAll, s.statsHandler.Lease(), statsTbl)
		return err
	}, util.FlagWrapTxn)
	return
}

// SaveColOrIdxStatsToStorage saves the stats to storage.
// If count is negative, both count and modify count would not be used and not be written to the table. Unless, corresponding
// fields in the stats_meta table will be updated.
// TODO: refactor to reduce the number of parameters
func (s *statsReadWriter) SaveColOrIdxStatsToStorage(
	tableID int64,
	count, modifyCount int64,
	isIndex int,
	hg *statistics.Histogram,
	cms *statistics.CMSketch,
	topN *statistics.TopN,
	statsVersion int,
	updateAnalyzeTime bool,
	source string,
) (err error) {
	var statsVer uint64
	start := time.Now()
	err = util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		statsVer, err = SaveColOrIdxStatsToStorage(sctx, tableID,
			count, modifyCount, isIndex, hg, cms, topN, statsVersion, updateAnalyzeTime)
		return err
	}, util.FlagWrapTxn)
	if err == nil && statsVer != 0 {
		// Check if saving was slow and update stats version if needed
		version, err2 := s.handleSlowStatsSaving(tableID, start)
		if err2 != nil {
			return err2
		}
		if version != 0 {
			statsVer = version
		}
		s.statsHandler.RecordHistoricalStatsMeta(statsVer, source, false, tableID)
	}
	return
}

// SaveMetaToStorage saves stats meta to the storage.
// Use the param `refreshLastHistVer` to indicate whether we need to update the last_histograms_versions in stats_meta table.
func (s *statsReadWriter) SaveMetaToStorage(
	source string,
	refreshLastHistVer bool,
	metaUpdates ...statstypes.MetaUpdate,
) (err error) {
	intest.Assert(len(metaUpdates) > 0, "meta updates is empty")
	var statsVer uint64
	err = util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		statsVer, err = SaveMetaToStorage(sctx, refreshLastHistVer, metaUpdates)
		return err
	}, util.FlagWrapTxn)
	if err == nil && statsVer != 0 {
		tableIDs := make([]int64, 0, len(metaUpdates))
		for i := range metaUpdates {
			tableIDs = append(tableIDs, metaUpdates[i].PhysicalID)
		}
		s.statsHandler.RecordHistoricalStatsMeta(statsVer, source, false, tableIDs...)
	}
	return
}

// InsertExtendedStats inserts a record into mysql.stats_extended and update version in mysql.stats_meta.
func (s *statsReadWriter) InsertExtendedStats(statsName string, colIDs []int64, tp int, tableID int64, ifNotExists bool) (err error) {
	var statsVer uint64
	err = util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		statsVer, err = InsertExtendedStats(sctx, s.statsHandler, statsName, colIDs, tp, tableID, ifNotExists)
		return err
	}, util.FlagWrapTxn)
	if err == nil && statsVer != 0 {
		s.statsHandler.RecordHistoricalStatsMeta(statsVer, "extended stats", false, tableID)
	}
	return
}

// MarkExtendedStatsDeleted update the status of mysql.stats_extended to be `deleted` and the version of mysql.stats_meta.
func (s *statsReadWriter) MarkExtendedStatsDeleted(statsName string, tableID int64, ifExists bool) (err error) {
	var statsVer uint64
	err = util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		statsVer, err = MarkExtendedStatsDeleted(sctx, s.statsHandler, statsName, tableID, ifExists)
		return err
	}, util.FlagWrapTxn)
	if err == nil && statsVer != 0 {
		s.statsHandler.RecordHistoricalStatsMeta(statsVer, "extended stats", false, tableID)
	}
	return
}

// SaveExtendedStatsToStorage writes extended stats of a table into mysql.stats_extended.
func (s *statsReadWriter) SaveExtendedStatsToStorage(tableID int64, extStats *statistics.ExtendedStatsColl, isLoad bool) (err error) {
	var statsVer uint64
	err = util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		statsVer, err = SaveExtendedStatsToStorage(sctx, tableID, extStats, isLoad)
		return err
	}, util.FlagWrapTxn)
	if err == nil && statsVer != 0 {
		s.statsHandler.RecordHistoricalStatsMeta(statsVer, "extended stats", false, tableID)
	}
	return
}

func (s *statsReadWriter) LoadTablePartitionStats(tableInfo *model.TableInfo, partitionDef *model.PartitionDefinition) (*statistics.Table, error) {
	var partitionStats *statistics.Table
	partitionStats, err := s.TableStatsFromStorage(tableInfo, partitionDef.ID, true, 0)
	if err != nil {
		return nil, err
	}
	// if the err == nil && partitionStats == nil, it means we lack the partition-level stats which the physicalID is equal to partitionID.
	if partitionStats == nil {
		errMsg := fmt.Sprintf("table `%s` partition `%s`", tableInfo.Name.L, partitionDef.Name.L)
		err = types.ErrPartitionStatsMissing.GenWithStackByArgs(errMsg)
		return nil, err
	}
	return partitionStats, nil
}

// LoadNeededHistograms will load histograms for those needed columns/indices.
func (s *statsReadWriter) LoadNeededHistograms(is infoschema.InfoSchema) (err error) {
	err = util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		loadFMSketch := config.GetGlobalConfig().Performance.EnableLoadFMSketch
		return LoadNeededHistograms(sctx, is, s.statsHandler, loadFMSketch)
	}, util.FlagWrapTxn)
	return err
}

// ReloadExtendedStatistics drops the cache for extended statistics and reload data from mysql.stats_extended.
func (s *statsReadWriter) ReloadExtendedStatistics() error {
	return util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		tables := make([]*statistics.Table, 0, s.statsHandler.Len())
		for _, tbl := range s.statsHandler.Values() {
			t, err := ExtendedStatsFromStorage(sctx, tbl.Copy(), tbl.PhysicalID, true)
			if err != nil {
				return err
			}
			tables = append(tables, t)
		}
		s.statsHandler.UpdateStatsCache(statstypes.CacheUpdate{
			Updated: tables,
		})
		return nil
	}, util.FlagWrapTxn)
}

// DumpStatsToJSON dumps statistic to json.
func (s *statsReadWriter) DumpStatsToJSON(dbName string, tableInfo *model.TableInfo,
	historyStatsExec sqlexec.RestrictedSQLExecutor, dumpPartitionStats bool) (*statsutil.JSONTable, error) {
	var snapshot uint64
	if historyStatsExec != nil {
		sctx := historyStatsExec.(sessionctx.Context)
		snapshot = sctx.GetSessionVars().SnapshotTS
	}
	return s.DumpStatsToJSONBySnapshot(dbName, tableInfo, snapshot, dumpPartitionStats)
}

// DumpHistoricalStatsBySnapshot dumped json tables from mysql.stats_meta_history and mysql.stats_history.
// As implemented in getTableHistoricalStatsToJSONWithFallback, if historical stats are nonexistent, it will fall back
// to the latest stats, and these table names (and partition names) will be returned in fallbackTbls.
func (s *statsReadWriter) DumpHistoricalStatsBySnapshot(
	dbName string,
	tableInfo *model.TableInfo,
	snapshot uint64,
) (
	jt *statsutil.JSONTable,
	fallbackTbls []string,
	err error,
) {
	historicalStatsEnabled, err := s.statsHandler.CheckHistoricalStatsEnable()
	if err != nil {
		return nil, nil, errors.Errorf("check %v failed: %v", vardef.TiDBEnableHistoricalStats, err)
	}
	if !historicalStatsEnabled {
		return nil, nil, errors.Errorf("%v should be enabled", vardef.TiDBEnableHistoricalStats)
	}

	defer func() {
		if err == nil {
			handle_metrics.DumpHistoricalStatsSuccessCounter.Inc()
		} else {
			handle_metrics.DumpHistoricalStatsFailedCounter.Inc()
		}
	}()
	pi := tableInfo.GetPartitionInfo()
	if pi == nil {
		jt, fallback, err := s.getTableHistoricalStatsToJSONWithFallback(dbName, tableInfo, tableInfo.ID, snapshot)
		if fallback {
			fallbackTbls = append(fallbackTbls, fmt.Sprintf("%s.%s", dbName, tableInfo.Name.O))
		}
		return jt, fallbackTbls, err
	}
	jsonTbl := &statsutil.JSONTable{
		DatabaseName: dbName,
		TableName:    tableInfo.Name.L,
		Partitions:   make(map[string]*statsutil.JSONTable, len(pi.Definitions)),
	}
	for _, def := range pi.Definitions {
		tbl, fallback, err := s.getTableHistoricalStatsToJSONWithFallback(dbName, tableInfo, def.ID, snapshot)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		if fallback {
			fallbackTbls = append(fallbackTbls, fmt.Sprintf("%s.%s %s", dbName, tableInfo.Name.O, def.Name.O))
		}
		jsonTbl.Partitions[def.Name.L] = tbl
	}
	tbl, fallback, err := s.getTableHistoricalStatsToJSONWithFallback(dbName, tableInfo, tableInfo.ID, snapshot)
	if err != nil {
		return nil, nil, err
	}
	if fallback {
		fallbackTbls = append(fallbackTbls, fmt.Sprintf("%s.%s global", dbName, tableInfo.Name.O))
	}
	// dump its global-stats if existed
	if tbl != nil {
		jsonTbl.Partitions[statsutil.TiDBGlobalStats] = tbl
	}
	return jsonTbl, fallbackTbls, nil
}

// PersistStatsBySnapshot dumps statistic to json and call the function for each partition statistic to persist.
// Notice:
//  1. It might call the function `persist` with nil jsontable.
//  2. It is only used by BR, so partitions' statistic are always dumped.
//
// TODO: once we support column-level statistic dump, it should replace the `PersistStatsBySnapshot` and `DumpStatsToJSON`.
func (s *statsReadWriter) PersistStatsBySnapshot(
	ctx context.Context,
	dbName string,
	tableInfo *model.TableInfo,
	snapshot uint64,
	persist statstypes.PersistFunc,
) error {
	pi := tableInfo.GetPartitionInfo()
	if pi == nil {
		jsonTable, err := s.TableStatsToJSON(dbName, tableInfo, tableInfo.ID, snapshot)
		if err != nil {
			return errors.Trace(err)
		}
		return persist(ctx, jsonTable, tableInfo.ID)
	}

	for _, def := range pi.Definitions {
		tbl, err := s.TableStatsToJSON(dbName, tableInfo, def.ID, snapshot)
		if err != nil {
			return errors.Trace(err)
		}
		if tbl == nil {
			continue
		}
		if err := persist(ctx, tbl, def.ID); err != nil {
			return errors.Trace(err)
		}
	}
	// dump its global-stats if existed
	tbl, err := s.TableStatsToJSON(dbName, tableInfo, tableInfo.ID, snapshot)
	if err != nil {
		return errors.Trace(err)
	}
	if tbl != nil {
		return persist(ctx, tbl, tableInfo.ID)
	}
	return nil
}

// DumpStatsToJSONBySnapshot dumps statistic to json.
func (s *statsReadWriter) DumpStatsToJSONBySnapshot(dbName string, tableInfo *model.TableInfo, snapshot uint64, dumpPartitionStats bool) (*statsutil.JSONTable, error) {
	pruneMode, err := util.GetCurrentPruneMode(s.statsHandler.SPool())
	if err != nil {
		return nil, err
	}
	isDynamicMode := variable.PartitionPruneMode(pruneMode) == variable.Dynamic
	pi := tableInfo.GetPartitionInfo()
	if pi == nil {
		return s.TableStatsToJSON(dbName, tableInfo, tableInfo.ID, snapshot)
	}
	jsonTbl := &statsutil.JSONTable{
		DatabaseName: dbName,
		TableName:    tableInfo.Name.L,
		Partitions:   make(map[string]*statsutil.JSONTable, len(pi.Definitions)),
	}
	// dump partition stats only if in static mode or enable dumpPartitionStats flag in dynamic mode
	if !isDynamicMode || dumpPartitionStats {
		for _, def := range pi.Definitions {
			tbl, err := s.TableStatsToJSON(dbName, tableInfo, def.ID, snapshot)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if tbl == nil {
				continue
			}
			jsonTbl.Partitions[def.Name.L] = tbl
		}
	}
	// dump its global-stats if existed
	tbl, err := s.TableStatsToJSON(dbName, tableInfo, tableInfo.ID, snapshot)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if tbl != nil {
		jsonTbl.Partitions[statsutil.TiDBGlobalStats] = tbl
	}
	return jsonTbl, nil
}

// getTableHistoricalStatsToJSONWithFallback try to get table historical stats, if not exist, directly fallback to the
// latest stats, and the second return value would be true.
func (s *statsReadWriter) getTableHistoricalStatsToJSONWithFallback(
	dbName string,
	tableInfo *model.TableInfo,
	physicalID int64,
	snapshot uint64,
) (
	*statsutil.JSONTable,
	bool,
	error,
) {
	jt, exist, err := s.tableHistoricalStatsToJSON(physicalID, snapshot)
	if err != nil {
		return nil, false, err
	}
	if !exist {
		jt, err = s.TableStatsToJSON(dbName, tableInfo, physicalID, 0)
		fallback := true
		if snapshot == 0 {
			fallback = false
		}
		return jt, fallback, err
	}
	return jt, false, nil
}

func (s *statsReadWriter) tableHistoricalStatsToJSON(physicalID int64, snapshot uint64) (jt *statsutil.JSONTable, exist bool, err error) {
	err = util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		jt, exist, err = TableHistoricalStatsToJSON(sctx, physicalID, snapshot)
		return err
	}, util.FlagWrapTxn)
	return
}

// TableStatsToJSON dumps statistic to json.
func (s *statsReadWriter) TableStatsToJSON(dbName string, tableInfo *model.TableInfo, physicalID int64, snapshot uint64) (*statsutil.JSONTable, error) {
	tbl, err := s.TableStatsFromStorage(tableInfo, physicalID, true, snapshot)
	if err != nil || tbl == nil {
		return nil, err
	}
	var jsonTbl *statsutil.JSONTable
	err = util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		tbl.Version, tbl.ModifyCount, tbl.RealtimeCount, err = StatsMetaByTableIDFromStorage(sctx, physicalID, snapshot)
		if err != nil {
			return err
		}
		// Note: Because we don't show this information in the session directly, so we can always use UTC here.
		colStatsUsage, err := predicatecolumn.LoadColumnStatsUsageForTable(sctx, time.UTC, physicalID)
		if err != nil {
			return err
		}
		jsonTbl, err = GenJSONTableFromStats(sctx, dbName, tableInfo, tbl, colStatsUsage)
		return err
	})
	if err != nil {
		return nil, err
	}
	return jsonTbl, nil
}

// TestLoadStatsErr is only for test.
type TestLoadStatsErr struct{}

// LoadStatsFromJSONConcurrently consumes concurrently the statistic task from `taskCh`.
func (s *statsReadWriter) LoadStatsFromJSONConcurrently(
	ctx context.Context,
	tableInfo *model.TableInfo,
	taskCh chan *statstypes.PartitionStatisticLoadTask,
	concurrencyForPartition int,
) error {
	nCPU := runtime.GOMAXPROCS(0)
	if concurrencyForPartition == 0 {
		concurrencyForPartition = (nCPU + 1) / 2 // default
	}
	concurrencyForPartition = min(concurrencyForPartition, nCPU) // for safety

	var wg sync.WaitGroup
	e := new(atomic.Pointer[error])
	for range concurrencyForPartition {
		wg.Add(1)
		s.statsHandler.GPool().Go(func() {
			defer func() {
				if r := recover(); r != nil {
					err := fmt.Errorf("%v", r)
					e.CompareAndSwap(nil, &err)
				}
				wg.Done()
			}()

			for tbl := range taskCh {
				if tbl == nil {
					continue
				}

				loadFunc := s.loadStatsFromJSON
				if intest.InTest && ctx.Value(TestLoadStatsErr{}) != nil {
					loadFunc = ctx.Value(TestLoadStatsErr{}).(func(*model.TableInfo, int64, *statsutil.JSONTable) error)
				}

				err := loadFunc(tableInfo, tbl.PhysicalID, tbl.JSONTable)
				if err != nil {
					e.CompareAndSwap(nil, &err)
					return
				}
				if e.Load() != nil {
					return
				}
			}
		})
	}
	wg.Wait()
	if e.Load() != nil {
		return *e.Load()
	}

	return nil
}

// LoadStatsFromJSONNoUpdate will load statistic from JSONTable, and save it to the storage.
func (s *statsReadWriter) LoadStatsFromJSONNoUpdate(ctx context.Context, is infoschema.InfoSchema,
	jsonTbl *statsutil.JSONTable, concurrencyForPartition int) error {
	table, err := is.TableByName(context.Background(), ast.NewCIStr(jsonTbl.DatabaseName), ast.NewCIStr(jsonTbl.TableName))
	if err != nil {
		return errors.Trace(err)
	}
	tableInfo := table.Meta()
	pi := tableInfo.GetPartitionInfo()
	if pi == nil || jsonTbl.Partitions == nil {
		err := s.loadStatsFromJSON(tableInfo, tableInfo.ID, jsonTbl)
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		// load partition statistics concurrently
		taskCh := make(chan *statstypes.PartitionStatisticLoadTask, len(pi.Definitions)+1)
		for _, def := range pi.Definitions {
			tbl := jsonTbl.Partitions[def.Name.L]
			if tbl != nil {
				taskCh <- &statstypes.PartitionStatisticLoadTask{
					PhysicalID: def.ID,
					JSONTable:  tbl,
				}
			}
		}

		// load global-stats if existed
		if globalStats, ok := jsonTbl.Partitions[statsutil.TiDBGlobalStats]; ok {
			taskCh <- &statstypes.PartitionStatisticLoadTask{
				PhysicalID: tableInfo.ID,
				JSONTable:  globalStats,
			}
		}
		close(taskCh)
		if err := s.LoadStatsFromJSONConcurrently(ctx, tableInfo, taskCh, concurrencyForPartition); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// LoadStatsFromJSON will load statistic from JSONTable, and save it to the storage.
// In final, it will also udpate the stats cache.
func (s *statsReadWriter) LoadStatsFromJSON(ctx context.Context, is infoschema.InfoSchema,
	jsonTbl *statsutil.JSONTable, concurrencyForPartition int) error {
	if err := s.LoadStatsFromJSONNoUpdate(ctx, is, jsonTbl, concurrencyForPartition); err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(s.statsHandler.Update(ctx, is))
}

func (s *statsReadWriter) loadStatsFromJSON(tableInfo *model.TableInfo, physicalID int64, jsonTbl *statsutil.JSONTable) error {
	tbl, err := TableStatsFromJSON(tableInfo, physicalID, jsonTbl)
	if err != nil {
		return errors.Trace(err)
	}

	var outerErr error
	tbl.ForEachColumnImmutable(func(_ int64, col *statistics.Column) bool {
		// loadStatsFromJSON doesn't support partition table now.
		// The table level count and modify_count would be overridden by the SaveMetaToStorage below, so we don't need
		// to care about them here.
		if err := s.SaveColOrIdxStatsToStorage(tbl.PhysicalID, tbl.RealtimeCount, 0, 0, &col.Histogram, col.CMSketch, col.TopN, int(col.GetStatsVer()), false, util.StatsMetaHistorySourceLoadStats); err != nil {
			outerErr = err
			return true
		}
		return false
	})
	if outerErr != nil {
		return outerErr
	}
	tbl.ForEachIndexImmutable(func(_ int64, idx *statistics.Index) bool {
		// loadStatsFromJSON doesn't support partition table now.
		// The table level count and modify_count would be overridden by the SaveMetaToStorage below, so we don't need
		// to care about them here.
		if err := s.SaveColOrIdxStatsToStorage(tbl.PhysicalID, tbl.RealtimeCount, 0, 1, &idx.Histogram, idx.CMSketch, idx.TopN, int(idx.GetStatsVer()), false, util.StatsMetaHistorySourceLoadStats); err != nil {
			outerErr = err
			return true
		}
		return false
	})
	if outerErr != nil {
		return outerErr
	}
	err = s.SaveExtendedStatsToStorage(tbl.PhysicalID, tbl.ExtendedStats, true)
	if err != nil {
		return errors.Trace(err)
	}
	err = s.SaveColumnStatsUsageToStorage(tbl.PhysicalID, jsonTbl.PredicateColumns)
	if err != nil {
		return errors.Trace(err)
	}
	return s.SaveMetaToStorage(util.StatsMetaHistorySourceLoadStats, true, statstypes.MetaUpdate{
		PhysicalID:  tbl.PhysicalID,
		Count:       tbl.RealtimeCount,
		ModifyCount: tbl.ModifyCount,
	})
}

// SaveColumnStatsUsageToStorage saves column statistics usage information for a table into mysql.column_stats_usage.
func (s *statsReadWriter) SaveColumnStatsUsageToStorage(physicalID int64, predicateColumns []*statsutil.JSONPredicateColumn) error {
	return util.CallWithSCtx(s.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		colStatsUsage := make(map[model.TableItemID]statstypes.ColStatsTimeInfo, len(predicateColumns))
		for _, col := range predicateColumns {
			if col == nil {
				continue
			}
			itemID := model.TableItemID{TableID: physicalID, ID: col.ID}
			lastUsedAt, err := parseTimeOrNil(col.LastUsedAt)
			if err != nil {
				return err
			}
			lastAnalyzedAt, err := parseTimeOrNil(col.LastAnalyzedAt)
			if err != nil {
				return err
			}
			colStatsUsage[itemID] = statstypes.ColStatsTimeInfo{
				LastUsedAt:     lastUsedAt,
				LastAnalyzedAt: lastAnalyzedAt,
			}
		}
		return predicatecolumn.SaveColumnStatsUsageForTable(sctx, colStatsUsage)
	}, util.FlagWrapTxn)
}

func parseTimeOrNil(timeStr *string) (*types.Time, error) {
	if timeStr == nil {
		return nil, nil
	}
	// DefaultStmtNoWarningContext use UTC timezone.
	parsedTime, err := types.ParseTime(types.DefaultStmtNoWarningContext, *timeStr, mysql.TypeTimestamp, types.MaxFsp)
	if err != nil {
		return nil, err
	}
	return &parsedTime, nil
}
