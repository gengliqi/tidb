// Copyright 2021 PingCAP, Inc.
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

package error //nolint: predeclared

import (
	"math"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	"github.com/stretchr/testify/assert"
	tikverr "github.com/tikv/client-go/v2/error"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/bazelbuild/rules_go/go/tools/bzltestutil.RegisterTimeoutHandler.func1"),
		goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	}
	goleak.VerifyTestMain(m, opts...)
}

func TestConvertError(t *testing.T) {
	wrapFuncs := []func(error) error{
		func(e error) error { return e },
		errors.Trace,
		errors.WithStack,
		func(e error) error { return errors.Wrap(e, "dummy") },
	}

	// All derived versions converts to `terror.ErrResultUndetermined`.
	e := tikverr.ErrResultUndetermined
	for _, f := range wrapFuncs {
		tidbErr := ToTiDBErr(f(e))
		assert.True(t, errors.ErrorEqual(tidbErr, terror.ErrResultUndetermined))
	}
}

func TestMemBufferOversizeError(t *testing.T) {
	err2str := map[error]string{
		&tikverr.ErrTxnTooLarge{Size: 100}:                   "Transaction is too large, size: 100",
		&tikverr.ErrEntryTooLarge{Limit: 10, Size: 20}:       "entry too large, the max entry size is 10, the size of data is 20",
		&tikverr.ErrKeyTooLarge{KeySize: math.MaxUint16 + 1}: "key is too large, the size of given key is 65536",
	}
	for err, errString := range err2str {
		tidbErr := ToTiDBErr(err)
		assert.NotNil(t, tidbErr)
		assert.Contains(t, tidbErr.Error(), errString)
	}
}
