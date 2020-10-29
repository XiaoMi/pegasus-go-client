// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package session

import (
	"context"
	"sync"
	"testing"

	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
)

// ensure context.cancel is able to interrupt the RPC.
func TestNodeSession_ContextCancel(t *testing.T) {
	defer leaktest.Check(t)()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	mm := NewMetaManager([]string{"0.0.0.0:34601"}, NewNodeSession)
	defer mm.Close()
	_, err := mm.QueryConfig(ctx, "temp")

	assert.Equal(t, err, ctx.Err())
}

func TestNodeSession_Call(t *testing.T) {
	defer leaktest.Check(t)()

	meta := newMetaSession("0.0.0.0:34601")
	defer meta.Close()

	_, err := meta.queryConfig(context.Background(), "temp")
	assert.Nil(t, err)
}

func TestMetaSession_MustQueryLeader(t *testing.T) {
	testMetaSessionMustQueryLeader(t, []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"})
	testMetaSessionMustQueryLeader(t, []string{"0.0.0.0:12345", "0.0.0.0:12346", "0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"})
}

func testMetaSessionMustQueryLeader(t *testing.T, metaServers []string) {
	defer leaktest.Check(t)()

	mm := NewMetaManager(metaServers, NewNodeSession)
	defer mm.Close()

	resp, err := mm.QueryConfig(context.Background(), "temp")
	assert.Nil(t, err)
	assert.Equal(t, resp.Err.Errno, base.ERR_OK.String())

	// the cached leader must be the actual leader
	ms := mm.metas[mm.currentLeader]
	ms.queryConfig(context.Background(), "temp")
	assert.Nil(t, err)
	assert.Equal(t, resp.Err.Errno, base.ERR_OK.String())
}

// Ensure that concurrent query_config calls won't make errors.
func TestNodeSession_ConcurrentCall(t *testing.T) {
	defer leaktest.Check(t)()

	meta := newMetaSession("0.0.0.0:34601")
	defer meta.Close()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			_, err := meta.queryConfig(context.Background(), "temp")
			assert.Nil(t, err)

			wg.Done()
		}()
	}
	wg.Wait()
}

// This test mocks the case that the first meta is unavailable. The MetaManager must be able to try
// communicating with the other metas.
func TestMetaManager_FirstMetaDead(t *testing.T) {
	defer leaktest.Check(t)()

	// the first meta is invalid
	mm := NewMetaManager([]string{"0.0.0.0:12345", "0.0.0.0:34603", "0.0.0.0:34602", "0.0.0.0:34601"}, NewNodeSession)
	defer mm.Close()

	resp, err := mm.QueryConfig(context.Background(), "temp")
	assert.Nil(t, err)
	assert.Equal(t, resp.Err.Errno, base.ERR_OK.String())
}
