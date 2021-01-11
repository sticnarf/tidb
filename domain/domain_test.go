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

package domain

import (
	"crypto/tls"
	"github.com/ngaut/pools"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util"
	"net"
	"testing"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
}

func mockFactory() (pools.Resource, error) {
	return nil, errors.New("mock factory should not be called")
}

func sysMockFactory(dom *Domain) (pools.Resource, error) {
	return nil, nil
}

type mockEtcdBackend struct {
	kv.Storage
	pdAddrs []string
}

func (mebd *mockEtcdBackend) EtcdAddrs() ([]string, error) {
	return mebd.pdAddrs, nil
}
func (mebd *mockEtcdBackend) TLSConfig() *tls.Config { return nil }
func (mebd *mockEtcdBackend) StartGCWorker() error {
	panic("not implemented")
}

// ETCD use ip:port as unix socket address, however this address is invalid on windows.
// We have to skip some of the test in such case.
// https://github.com/etcd-io/etcd/blob/f0faa5501d936cd8c9f561bb9d1baca70eb67ab1/pkg/types/urls.go#L42
func unixSocketAvailable() bool {
	c, err := net.Listen("unix", "127.0.0.1:0")
	if err == nil {
		c.Close()
		return true
	}
	return false
}

type mockSessionManager struct {
	PS []*util.ProcessInfo
}

func (msm *mockSessionManager) ShowProcessList() map[uint64]*util.ProcessInfo {
	ret := make(map[uint64]*util.ProcessInfo)
	for _, item := range msm.PS {
		ret[item.ID] = item
	}
	return ret
}

func (msm *mockSessionManager) GetProcessInfo(id uint64) (*util.ProcessInfo, bool) {
	for _, item := range msm.PS {
		if item.ID == id {
			return item, true
		}
	}
	return &util.ProcessInfo{}, false
}

func (msm *mockSessionManager) Kill(cid uint64, query bool) {}

func (msm *mockSessionManager) UpdateTLSConfig(cfg *tls.Config) {}

type testResource struct {
	status int
}

func (tr *testResource) Close() { tr.status = 1 }

func (*testSuite) TestSessionPool(c *C) {
	f := func() (pools.Resource, error) { return &testResource{}, nil }
	pool := newSessionPool(1, f)
	tr, err := pool.Get()
	c.Assert(err, IsNil)
	tr1, err := pool.Get()
	c.Assert(err, IsNil)
	pool.Put(tr)
	// Capacity is 1, so tr1 is closed.
	pool.Put(tr1)
	c.Assert(tr1.(*testResource).status, Equals, 1)
	pool.Close()

	pool.Close()
	pool.Put(tr1)
	tr, err = pool.Get()
	c.Assert(err.Error(), Equals, "session pool closed")
	c.Assert(tr, IsNil)
}

func (*testSuite) TestErrorCode(c *C) {
	c.Assert(int(terror.ToSQLError(ErrInfoSchemaExpired).Code), Equals, errno.ErrInfoSchemaExpired)
	c.Assert(int(terror.ToSQLError(ErrInfoSchemaChanged).Code), Equals, errno.ErrInfoSchemaChanged)
}
