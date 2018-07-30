// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package pegasus

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"bytes"
	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/XiaoMi/pegasus-go-client/idl/replication"
	"github.com/XiaoMi/pegasus-go-client/rpc"
	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
)

// This is the integration test of the client. Please start the pegasus onebox
// before you running the tests.

func testSingleKeyOperations(t *testing.T, tb TableConnector, hashKey []byte, sortKey []byte, value []byte) {
	// read after write
	assert.Nil(t, tb.Del(context.Background(), hashKey, sortKey))
	assert.Nil(t, tb.Set(context.Background(), hashKey, sortKey, value))
	result, err := tb.Get(context.Background(), hashKey, sortKey)
	assert.Nil(t, err)
	assert.Equal(t, value, result)
	exist, err := tb.Exist(context.Background(), hashKey, sortKey)
	assert.Nil(t, err)
	assert.Equal(t, true, exist)

	// ensure GET a non-existed entry returns a nil value
	assert.Nil(t, tb.Del(context.Background(), hashKey, sortKey))
	result = nil
	result, err = tb.Get(context.Background(), hashKey, sortKey)
	assert.Nil(t, err)
	assert.Nil(t, result)
	exist, err = tb.Exist(context.Background(), hashKey, sortKey)
	assert.Nil(t, err)
	assert.Equal(t, false, exist)

	// === ttl === //

	ttl, err := tb.TTL(context.Background(), hashKey, sortKey)
	assert.Nil(t, err)
	assert.Equal(t, ttl, -2)

	assert.Nil(t, tb.Set(context.Background(), hashKey, sortKey, value))
	ttl, err = tb.TTL(context.Background(), hashKey, sortKey)
	assert.Nil(t, err)
	assert.Equal(t, ttl, -1)

	assert.Nil(t, tb.SetTTL(context.Background(), hashKey, sortKey, value, time.Second*10))
	ttl, err = tb.TTL(context.Background(), hashKey, sortKey)
	assert.Nil(t, err)
	assert.Condition(t, func() bool {
		// pegasus server may return a ttl slightly different
		// from the value we set.
		return ttl <= 11 && ttl >= 9
	})

	assert.Nil(t, tb.Del(context.Background(), hashKey, sortKey))
}

func TestPegasusTableConnector_SingleKeyOperations(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}
	client := NewClient(cfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()

	// run sequentially
	for i := 0; i < 1000; i++ {
		hashKey := []byte(fmt.Sprintf("h%d", i))
		sortKey := []byte(fmt.Sprintf("s%d", i))
		value := []byte(fmt.Sprintf("v%d", i))
		testSingleKeyOperations(t, tb, hashKey, sortKey, value)
	}

	// run concurrently
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)

		id := i
		go func() {
			hashKey := []byte(fmt.Sprintf("h%d", id))
			sortKey := []byte(fmt.Sprintf("s%d", id))
			value := []byte(fmt.Sprintf("v%d", id))

			testSingleKeyOperations(t, tb, hashKey, sortKey, value)
			wg.Done()
		}()
	}
	wg.Wait()
}

// ensure client will return InvalidArguments errors when input is empty.
func TestPegasusTableConnector_EmptyInput(t *testing.T) {
	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}
	client := NewClient(cfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()

	// === empty hashkey === //

	_, err = tb.TTL(context.Background(), nil, nil)
	assert.NotNil(t, err)
	_, err2 := tb.TTL(context.Background(), []byte{}, nil)
	assert.Equal(t, err, err2)

	_, err = tb.Exist(context.Background(), nil, nil)
	assert.NotNil(t, err)
	_, err2 = tb.Exist(context.Background(), []byte{}, nil)
	assert.Equal(t, err, err2)

	err = tb.SetTTL(context.Background(), nil, nil, nil, 0)
	assert.NotNil(t, err)
	err2 = tb.Set(context.Background(), []byte{}, nil, nil)
	assert.Equal(t, err, err2)

	_, err = tb.Get(context.Background(), nil, nil)
	assert.NotNil(t, err)
	_, err2 = tb.Get(context.Background(), []byte{}, nil)
	assert.Equal(t, err, err2)

	_, _, err = tb.MultiGet(context.Background(), nil, nil)
	assert.NotNil(t, err)
	_, _, err2 = tb.MultiGetOpt(context.Background(), []byte{}, nil, &MultiGetOptions{})
	assert.Equal(t, err, err2)

	_, _, err = tb.MultiGetRange(context.Background(), nil, nil, nil)
	assert.NotNil(t, err)
	_, _, err2 = tb.MultiGetRangeOpt(context.Background(), []byte{}, nil, nil, &MultiGetOptions{})
	assert.Equal(t, err, err2)

	err = tb.MultiSet(context.Background(), nil, nil, nil)
	assert.NotNil(t, err)
	err2 = tb.MultiSetOpt(context.Background(), []byte{}, nil, nil, 0)
	assert.Equal(t, err, err2)

	err = tb.MultiDel(context.Background(), nil, nil)
	assert.NotNil(t, err)
	err2 = tb.MultiDel(context.Background(), []byte{}, nil)
	assert.Equal(t, err, err2)

	err = tb.Del(context.Background(), nil, nil)
	assert.NotNil(t, err)
	err2 = tb.Del(context.Background(), []byte{}, nil)
	assert.Equal(t, err, err2)

	// === empty value === //

	err = tb.SetTTL(context.Background(), []byte("h1"), nil, nil, 0)
	assert.NotNil(t, err)
	err2 = tb.Set(context.Background(), []byte("h1"), nil, []byte{})
	assert.Equal(t, err, err2)

	err = tb.MultiSet(context.Background(), []byte("h1"), nil, nil)
	assert.NotNil(t, err)
	err2 = tb.MultiSetOpt(context.Background(), []byte("h1"), [][]byte{}, [][]byte{}, 0)
	assert.Equal(t, err, err2)

	err = tb.MultiSet(context.Background(), []byte("h1"), [][]byte{[]byte("s1")}, [][]byte{nil})
	assert.NotNil(t, err)
	err2 = tb.MultiSetOpt(context.Background(), []byte("h1"), [][]byte{[]byte("s1")}, [][]byte{[]byte{}}, 0)
	assert.Equal(t, err, err2)
}

func TestPegasusTableConnector_TriggerSelfUpdate(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}

	client := NewClient(cfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	ptb, _ := tb.(*pegasusTableConnector)

	err = ptb.handleReplicaError(nil, nil, nil)
	assert.Nil(t, err)

	ptb.handleReplicaError(errors.New("not nil"), nil, nil)
	<-ptb.confUpdateCh

	ptb.handleReplicaError(base.ERR_OBJECT_NOT_FOUND, nil, nil)
	<-ptb.confUpdateCh

	ptb.handleReplicaError(base.ERR_INVALID_STATE, nil, nil)
	<-ptb.confUpdateCh

	{ // Ensure: The following errors should not trigger configuration update
		errorTypes := []error{base.ERR_TIMEOUT, context.DeadlineExceeded, base.ERR_CAPACITY_EXCEEDED, base.ERR_NOT_ENOUGH_MEMBER}

		for _, err := range errorTypes {
			channelEmpty := false
			ptb.handleReplicaError(err, nil, nil)
			select {
			case <-ptb.confUpdateCh:
			default:
				channelEmpty = true
			}
			assert.True(t, channelEmpty)
		}
	}
}

func TestPegasusTableConnector_ValidateHashKey(t *testing.T) {
	var hashKey []byte

	hashKey = nil
	assert.NotNil(t, validateHashKey(hashKey))

	hashKey = make([]byte, 0)
	assert.NotNil(t, validateHashKey(hashKey))

	hashKey = make([]byte, math.MaxUint16+1)
	assert.NotNil(t, validateHashKey(hashKey))
}

func TestPegasusTableConnector_HandleInvalidQueryConfigResp(t *testing.T) {
	defer leaktest.Check(t)()

	p := &pegasusTableConnector{
		tableName: "temp",
	}

	{
		resp := replication.NewQueryCfgResponse()
		resp.Err = &base.ErrorCode{Errno: "ERR_BUSY"}

		err := p.handleQueryConfigResp(resp)
		assert.NotNil(t, err)
		assert.Equal(t, err.Error(), "ERR_BUSY")
	}

	{
		resp := replication.NewQueryCfgResponse()
		resp.Err = &base.ErrorCode{Errno: "ERR_OK"}

		err := p.handleQueryConfigResp(resp)
		assert.NotNil(t, err)

		resp.Partitions = make([]*replication.PartitionConfiguration, 10)
		resp.PartitionCount = 5
		err = p.handleQueryConfigResp(resp)
		assert.NotNil(t, err)
	}

	{
		resp := replication.NewQueryCfgResponse()
		resp.Err = &base.ErrorCode{Errno: "ERR_OK"}

		resp.Partitions = make([]*replication.PartitionConfiguration, 4)
		resp.PartitionCount = 4

		err := p.handleQueryConfigResp(resp)
		assert.NotNil(t, err)
		assert.Equal(t, len(p.parts), 4)
	}
}

func TestPegasusTableConnector_Close(t *testing.T) {
	// Ensure loopForAutoUpdate will be closed.
	defer leaktest.Check(t)()

	// Ensure: Closing table doesn't close the connections.

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}

	client := NewClient(cfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	ptb, _ := tb.(*pegasusTableConnector)

	err = tb.Set(context.Background(), []byte("a"), []byte("a"), []byte("a"))
	assert.Nil(t, err)

	ptb.Close()
	_, r := ptb.getPartition([]byte("a"))
	assert.Equal(t, r.ConnState(), rpc.ConnStateReady)
}

func TestPegasusTableConnector_MultiKeyOperations(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}
	client := NewClient(cfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()

	testMultiKeyOperations(t, tb)
}

func testMultiKeyOperations(t *testing.T, tb TableConnector) {
	hashKey := []byte("h1")

	sortKeys := make([][]byte, 100)
	values := make([][]byte, 100)
	for i := 0; i < 100; i++ {
		// make sortKeys sorted.
		sidBuf := []byte(fmt.Sprintf("%d", i))
		var sidWithLeadingZero bytes.Buffer
		for i := 0; i < 20-len(sidBuf); i++ {
			sidWithLeadingZero.WriteByte('0')
		}
		sidWithLeadingZero.Write(sidBuf)
		sortKeys[i] = sidWithLeadingZero.Bytes()
		values[i] = []byte(fmt.Sprintf("v%d", i))
	}

	// empty keys
	assert.Nil(t, tb.MultiDel(context.Background(), hashKey, sortKeys))
	results, allFetched, err := tb.MultiGet(context.Background(), hashKey, sortKeys)
	assert.Nil(t, err)
	assert.Nil(t, results)
	assert.True(t, allFetched)

	// === read after write === //

	assert.Nil(t, tb.MultiSet(context.Background(), hashKey, sortKeys, values))

	results, allFetched, err = tb.MultiGet(context.Background(), hashKey, sortKeys)
	assert.Nil(t, err)
	assert.Equal(t, len(results), len(values))
	for i, result := range results {
		assert.Equal(t, result.Value, values[i])
		assert.Equal(t, result.SortKey, sortKeys[i])
	}
	assert.True(t, allFetched)

	results, allFetched, err = tb.MultiGetRangeOpt(context.Background(), hashKey, sortKeys[0], sortKeys[len(sortKeys)-1],
		&MultiGetOptions{StartInclusive: true, StopInclusive: true})
	assert.Nil(t, err)
	assert.Equal(t, len(results), len(values))
	for i, result := range results {
		assert.Equal(t, result.Value, values[i])
		assert.Equal(t, result.SortKey, sortKeys[i])
	}
	assert.True(t, allFetched)

	results, allFetched, err = tb.MultiGetRangeOpt(context.Background(), hashKey, sortKeys[0], sortKeys[len(sortKeys)-1],
		&MultiGetOptions{StartInclusive: false, StopInclusive: false})
	assert.Nil(t, err)
	assert.Equal(t, len(results), len(values)-2) // exclude start and stop
	for i, result := range results {
		assert.Equal(t, result.Value, values[i+1])
		assert.Equal(t, result.SortKey, sortKeys[i+1])
	}
	assert.True(t, allFetched)

	results, allFetched, err = tb.MultiGetOpt(context.Background(), hashKey, sortKeys, &MultiGetOptions{MaxFetchCount: 4})
	assert.Nil(t, err)
	assert.Equal(t, len(results), 4)
	assert.False(t, allFetched)

	results, allFetched, err = tb.MultiGetOpt(context.Background(), hashKey, sortKeys, &MultiGetOptions{MaxFetchSize: len(values[0])})
	assert.Nil(t, err)
	assert.Equal(t, len(results), 1)
	assert.False(t, allFetched)

	// === ttl === //

	assert.Nil(t, tb.MultiSetOpt(context.Background(), hashKey, sortKeys, values, 10*time.Second))
	for _, sortKey := range sortKeys {
		ttl, err := tb.TTL(context.Background(), hashKey, sortKey)
		assert.Nil(t, err)
		assert.Condition(t, func() bool {
			// pegasus server may return a ttl slightly different
			// from the value we set.
			return ttl <= 11 && ttl >= 9
		})
	}
}

func TestPegasusTableConnector_ScanAllSortKey(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}
	client := NewClient(cfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()

	baseMap := make(map[string]map[string]string)
	clearDatabase(t, tb)
	setDatabase(tb, baseMap)

	options := &ScannerOptions{
		BatchSize:      1000,
		StartInclusive: true,
		HashKeyFilter:  Filter{FilterTypeMatchPrefix, []byte("")},
		SortKeyFilter:  Filter{FilterTypeMatchPrefix, []byte("")},
	}
	scanner, err := tb.GetScanner(context.Background(), []byte("h1"), []byte{}, []byte{}, options)
	assert.Nil(t, err)

	dataMap := make(map[string]string)
	for {
		err, completed, h, s, v := scanner.Next(context.Background())
		assert.Nil(t, err)
		if completed {
			break
		}
		assert.Equal(t, []byte("h1"), h)
		_, ok := dataMap[string(s)]
		assert.False(t, ok)
		dataMap[string(s)] = string(v)
	}
	scanner.Close()
	compareMaps(t, dataMap, baseMap["h1"])
}

func TestPegasusTableConnector_ScanInclusive(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}
	client := NewClient(cfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()

	baseMap := make(map[string]map[string]string)
	clearDatabase(t, tb)
	setDatabase(tb, baseMap)

	var start, stop []byte
	for s := range baseMap["h1"] {
		start = []byte(s)
		break
	}
	for s := range baseMap["h1"] {
		stop = []byte(s)
		break
	}
	if string(start) > string(stop) {
		temp := stop
		stop = start
		start = temp
	}

	options := &ScannerOptions{
		BatchSize:      1000,
		StartInclusive: true,
		StopInclusive:  true,
		HashKeyFilter:  Filter{FilterTypeMatchPrefix, []byte("")},
		SortKeyFilter:  Filter{FilterTypeMatchPrefix, []byte("")},
	}

	scanner, err := tb.GetScanner(context.Background(), []byte("h1"), start, stop, options)
	assert.Nil(t, err)

	dataMap := make(map[string]string)
	ctx, _ := context.WithTimeout(context.Background(), time.Second*5)
	for {
		err, completed, h, s, v := scanner.Next(ctx)
		assert.Nil(t, err)
		if completed {
			break
		}
		assert.Equal(t, []byte("h1"), h)
		_, ok := dataMap[string(s)]
		assert.False(t, ok)
		dataMap[string(s)] = string(v)
	}
	scanner.Close()

	cutAndCompareMaps(t, dataMap, baseMap["h1"], start, true, stop, true)
}

func TestPegasusTableConnector_ScanExclusive(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}
	client := NewClient(cfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()

	baseMap := make(map[string]map[string]string)
	clearDatabase(t, tb)
	setDatabase(tb, baseMap)

	var start, stop []byte
	for s := range baseMap["h1"] {
		start = []byte(s)
		break
	}
	for s := range baseMap["h1"] {
		if s == string(start) {
			continue
		}
		stop = []byte(s)
		break
	}
	if string(start) > string(stop) {
		temp := stop
		stop = start
		start = temp
	}

	options := &ScannerOptions{
		BatchSize:      1000,
		StartInclusive: false,
		StopInclusive:  false,
		HashKeyFilter:  Filter{FilterTypeMatchPrefix, []byte("")},
		SortKeyFilter:  Filter{FilterTypeMatchPrefix, []byte("")},
	}

	scanner, err := tb.GetScanner(context.Background(), []byte("h1"), start, stop, options)
	assert.Nil(t, err)
	assert.NotNil(t, scanner)
	dataMap := make(map[string]string)
	ctx, _ := context.WithTimeout(context.Background(), time.Second*5)
	for {
		err, completed, h, s, v := scanner.Next(ctx)
		assert.Nil(t, err)
		if completed {
			break
		}
		assert.Equal(t, []byte("h1"), h)
		_, ok := dataMap[string(s)]
		assert.False(t, ok)
		dataMap[string(s)] = string(v)
	}
	scanner.Close()

	err = cutAndCompareMaps(t, dataMap, baseMap["h1"], start, false, stop, false)
	//fmt.Println(err.Error()) if can't cut the baseMap, abandon compare
}

func TestPegasusTableConnector_ScanOnePoint(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}
	client := NewClient(cfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()

	baseMap := make(map[string]map[string]string)
	clearDatabase(t, tb)
	setDatabase(tb, baseMap)

	var start []byte
	for s := range baseMap["h1"] {
		start = []byte(s)
		break
	}

	options := NewScanOptions()
	options.StartInclusive = true
	options.StopInclusive = true
	scanner, err := tb.GetScanner(context.Background(), []byte("h1"), start, start, options)
	assert.Nil(t, err)
	err, completed, h, s, v := scanner.Next(context.Background())
	assert.Nil(t, err)
	assert.False(t, completed)
	assert.Equal(t, []byte("h1"), h)
	assert.Equal(t, start, s)
	assert.Equal(t, baseMap["h1"][string(start)], string(v))

	err, completed, _, _, _ = scanner.Next(context.Background())
	assert.Nil(t, err)
	assert.True(t, completed)
	scanner.Close()
}

func TestPegasusTableConnector_ScanHalfInclusive(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}
	client := NewClient(cfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()

	baseMap := make(map[string]map[string]string)
	clearDatabase(t, tb)
	setDatabase(tb, baseMap)

	var start []byte
	for s := range baseMap["h1"] {
		start = []byte(s)
		break
	}

	options := NewScanOptions()
	options.StartInclusive = true
	options.StopInclusive = false
	_, err = tb.GetScanner(context.Background(), []byte("h1"), start, start, options)
	assert.NotNil(t, err)
}

func TestPegasusTableConnector_ScanVoidSpan(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}
	client := NewClient(cfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()

	baseMap := make(map[string]map[string]string)
	clearDatabase(t, tb)
	setDatabase(tb, baseMap)

	var start, stop []byte
	for s := range baseMap["h1"] {
		start = []byte(s)
		break
	}
	for s := range baseMap["h1"] {
		if s == string(start) {
			continue
		}
		stop = []byte(s)
		break
	}
	if string(start) > string(stop) {
		temp := stop
		stop = start
		start = temp
	}

	options := NewScanOptions()
	options.StartInclusive = true
	options.StopInclusive = true
	_, err = tb.GetScanner(context.Background(), []byte("h1"), stop, start, options)
	assert.NotNil(t, err)
}

func TestPegasusTableConnector_ScanOverallScan(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}
	client := NewClient(cfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()

	baseMap := make(map[string]map[string]string)
	clearDatabase(t, tb)
	setDatabase(tb, baseMap)

	options := NewScanOptions()
	dataMap := make(map[string]string)

	scanners, err := tb.GetUnorderedScanners(context.Background(), 3, options)
	assert.Nil(t, err)
	assert.True(t, len(scanners) <= 3)

	for _, s := range scanners {
		assert.NotNil(t, s)
		for {
			err, completed, h, s, v := s.Next(context.Background())
			assert.Nil(t, err)
			if completed {
				break
			}

			blob := encodeHashKeySortKey(h, s)
			dataMap[string(blob.Data)] = string(v)
		}
		s.Close()
	}

	compareAll(t, dataMap, baseMap)
}

func TestPegasusTableConnector_ConcurrentCallScanner(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}
	client := NewClient(cfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()

	baseMap := make(map[string]map[string]string)
	clearDatabase(t, tb)
	setDatabase(tb, baseMap)

	batchSizes := []int{10, 100, 500, 1000}

	var wg sync.WaitGroup
	for i := 0; i < len(batchSizes); i++ {
		wg.Add(1)
		batchSize := batchSizes[i]
		options := NewScanOptions()
		options.BatchSize = batchSize

		dataMap := make(map[string]string)
		scanners, err := tb.GetUnorderedScanners(context.Background(), 1, options)
		assert.Nil(t, err)
		assert.True(t, len(scanners) <= 1)

		scanner := scanners[0]
		for {
			err, completed, h, s, v := scanner.Next(context.Background())
			assert.Nil(t, err)
			if completed {
				break
			}
			blob := encodeHashKeySortKey(h, s)
			dataMap[string(blob.Data)] = string(v)
		}
		scanner.Close()
		compareAll(t, dataMap, baseMap)
		wg.Done()
	}
	wg.Wait()
}

func TestPegasusTableConnector_NoValueScan(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}
	client := NewClient(cfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()

	baseMap := make(map[string]map[string]string)
	clearDatabase(t, tb)
	setDatabase(tb, baseMap)

	options := &ScannerOptions{
		BatchSize:      1000,
		StartInclusive: true,
		HashKeyFilter:  Filter{FilterTypeMatchPrefix, []byte("")},
		SortKeyFilter:  Filter{FilterTypeMatchPrefix, []byte("")},
	}
	options.NoValue = true
	scanner, err := tb.GetScanner(context.Background(), []byte("h1"), []byte{}, []byte{}, options)
	assert.Nil(t, err)

	ctx, _ := context.WithTimeout(context.Background(), time.Second*5)
	for {
		err, completed, h, s, v := scanner.Next(ctx)
		assert.Nil(t, err)
		if completed {
			break
		}
		assert.Equal(t, []byte("h1"), h)
		_, ok := baseMap["h1"][string(s)]
		assert.True(t, ok)
		assert.True(t, len(v) == 0)
	}
	scanner.Close()
}

func clearDatabase(t *testing.T, tb TableConnector) {
	options := NewScanOptions()
	scanners, err := tb.GetUnorderedScanners(context.Background(), 1, options)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(scanners))
	assert.NotNil(t, scanners[0])

	for {
		err1, completed, h, s, _ := scanners[0].Next(context.Background())
		assert.Nil(t, err1)
		if completed {
			break
		}
		err = tb.Del(context.Background(), h, s)
		assert.Nil(t, err)
	}

	scanners[0].Close()

	scanners, err = tb.GetUnorderedScanners(context.Background(), 1, options)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(scanners))
	assert.NotNil(t, scanners[0])
	err, completed, _, _, _ := scanners[0].Next(context.Background())
	assert.Nil(t, err)
	assert.True(t, completed)
}

func setDatabase(tb TableConnector, baseMap map[string]map[string]string) {
	hashMap := make(map[string]string)
	for i := 0; i < 10 || len(hashMap) < 10; i++ {
		s := randomBytes(100)
		v := randomBytes(100)
		tb.Set(context.Background(), []byte("h1"), s, v)
		hashMap[string(s)] = string(v)
	}
	baseMap["h1"] = hashMap

	for i := 0; i < 100 || len(baseMap) < 100; i++ {
		h := randomBytes(100)
		sortMap, ok := baseMap[string(h)]
		if !ok {
			sortMap = make(map[string]string)
			baseMap[string(h)] = sortMap
		}
		for j := 0; j < 10 || len(sortMap) < 10; j++ {
			s := randomBytes(100)
			v := randomBytes(100)
			tb.Set(context.Background(), h, s, v)
			sortMap[string(s)] = string(v)
		}

	}
}

func TestPegasusTableConnector_CheckAndSet(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}
	client := NewClient(cfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()

	{ // CheckTypeValueNotExist
		// if (h1, s1) not exists, insert (s1, v1)
		err := tb.Del(context.Background(), []byte("h1"), []byte("s1"))
		assert.Nil(t, err)
		res, err := tb.CheckAndSet(context.Background(), []byte("h1"), []byte("s1"), CheckTypeValueNotExist, []byte(""), []byte("s1"), []byte("v1"),
			&CheckAndSetOptions{ReturnCheckValue: true})
		assert.Nil(t, err)
		assert.Equal(t, res.SetSucceed, true)
		assert.Equal(t, res.CheckValueReturned, true)
		assert.Equal(t, res.CheckValueExist, false)

		// since (h1, s1) exists, insertion of (s1, v1) failed
		res, err = tb.CheckAndSet(context.Background(), []byte("h1"), []byte("s1"), CheckTypeValueNotExist, []byte(""), []byte("s1"), []byte("v1"),
			&CheckAndSetOptions{ReturnCheckValue: true})
		assert.Nil(t, err)
		assert.Equal(t, res.SetSucceed, false)
		assert.Equal(t, res.CheckValueReturned, true)
		assert.Equal(t, res.CheckValueExist, true)
		assert.Equal(t, res.CheckValue, []byte("v1"))
	}

	{ // CheckTypeValueExist
		// if (h1, s1) exists, insert (s1, v1)
		// this op will failed since there's no such entry.
		assert.Nil(t, tb.Del(context.Background(), []byte("h1"), []byte("s1")))
		res, err := tb.CheckAndSet(context.Background(), []byte("h1"), []byte("s1"), CheckTypeValueExist, []byte(""), []byte("s1"), []byte("v1"),
			&CheckAndSetOptions{ReturnCheckValue: true})
		assert.Nil(t, err)
		assert.Equal(t, res.SetSucceed, false)
		assert.Equal(t, res.CheckValueReturned, true)
		assert.Equal(t, res.CheckValueExist, false)

		assert.Nil(t, tb.Set(context.Background(), []byte("h1"), []byte("s1"), []byte("v1")))
		res, err = tb.CheckAndSet(context.Background(), []byte("h1"), []byte("s1"), CheckTypeValueExist, []byte(""), []byte("s1"), []byte("v2"),
			&CheckAndSetOptions{ReturnCheckValue: true})
		assert.Nil(t, err)
		assert.Equal(t, res.SetSucceed, true)
		assert.Equal(t, res.CheckValueReturned, true)
		assert.Equal(t, res.CheckValueExist, true)
		assert.Equal(t, res.CheckValue, []byte("v1"))

		value, err := tb.Get(context.Background(), []byte("h1"), []byte("s1"))
		assert.Nil(t, err)
		assert.Equal(t, value, []byte("v2"))

		// set ttl to 10 if value exists
		ttl, err := tb.TTL(context.Background(), []byte("h1"), []byte("s1"))
		assert.Nil(t, err)
		assert.Equal(t, ttl, 0)

		res, err = tb.CheckAndSet(context.Background(), []byte("h1"), []byte("s1"), CheckTypeValueExist, []byte(""), []byte("s1"), []byte("v3"),
			&CheckAndSetOptions{SetValueTTLSeconds: 10})
		assert.Nil(t, err)
		assert.Equal(t, res.SetSucceed, true)
		assert.Equal(t, res.CheckValueReturned, true)
		assert.Equal(t, res.CheckValueExist, true)

		ttl, err = tb.TTL(context.Background(), []byte("h1"), []byte("s1"))
		assert.Nil(t, err)
		assert.Equal(t, ttl, 10)
	}

	// TODO(wutao1): add tests for other check type
}
