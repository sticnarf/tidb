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
// See the License for the specific language governing permissions and
// limitations under the License.

package ddltest

import (
	"fmt"
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	goctx "golang.org/x/net/context"
	"io"
	"sync"
	"sync/atomic"
)

func getIndex(t table.Table, name string) table.Index {
	for _, idx := range t.Indices() {
		if idx.Meta().Name.O == name {
			return idx
		}
	}

	return nil
}

func (s *TestDDLSuite) checkAddIndex(c *C, indexInfo *model.IndexInfo) {
	ctx := s.ctx
	err := ctx.NewTxn(goctx.Background())
	c.Assert(err, IsNil)
	tbl := s.getTable(c, "test_index")

	// read handles form table
	handles := make(map[int64]struct{})
	err = tbl.IterRecords(ctx, tbl.FirstKey(), tbl.Cols(),
		func(h int64, data []types.Datum, cols []*table.Column) (bool, error) {
			handles[h] = struct{}{}
			return true, nil
		})
	c.Assert(err, IsNil)

	// read handles from index
	idx := tables.NewIndex(tbl.Meta().ID, tbl.Meta(), indexInfo)
	err = ctx.NewTxn(goctx.Background())
	c.Assert(err, IsNil)
	txn, err := ctx.Txn(false)
	c.Assert(err, IsNil)
	defer func() {
		txn.Rollback()
	}()

	it, err := idx.SeekFirst(txn)
	c.Assert(err, IsNil)
	defer it.Close()

	for {
		_, h, err := it.Next()
		if terror.ErrorEqual(err, io.EOF) {
			break
		}

		c.Assert(err, IsNil)
		c.Assert(handles, HasKey, h)
		delete(handles, h)
	}

	c.Assert(handles, HasLen, 0)
}

func (s *TestDDLSuite) execIndexOperations(c *C, workerNum, count int, insertID *int64) {
	var wg sync.WaitGroup
	// workerNum = 10
	wg.Add(workerNum)
	for i := 0; i < workerNum; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < count; j++ {
				id := atomic.AddInt64(insertID, 1)
				sql := fmt.Sprintf("insert into test_index values (%d, %d, %f, '%s')", id, randomInt(), randomFloat(), randomString(10))
				s.execInsert(c, sql)
				c.Logf("sql %s", sql)
				sql = fmt.Sprintf("delete from test_index where c = %d", randomIntn(int(id)))
				s.mustExec(c, sql)
				c.Logf("sql %s", sql)
				sql = fmt.Sprintf("update test_index set c1 = %d, c2 = %f, c3 = '%s' where c = %d", randomInt(), randomFloat(), randomString(10), randomIntn(int(id)))
				s.mustExec(c, sql)
				c.Logf("sql %s", sql)

			}
		}()
	}
	wg.Wait()
}
