package pegasus

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
)

func clearDatabase(t *testing.T, tb TableConnector) {
	simpleFullScan(t, tb, func(hashKey, sortKey, value []byte) {
		err := tb.Del(context.Background(), hashKey, sortKey)
		assert.Nil(t, err)
	})
}

func simpleFullScanOpts(t *testing.T, tb TableConnector, handler func(hashKey, sortKey, value []byte), options *ScannerOptions) {
	scanners, err := tb.GetUnorderedScanners(context.Background(), 1, options)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(scanners))
	assert.NotNil(t, scanners[0])

	for {
		completed, h, s, v, err := scanners[0].Next(context.Background())
		assert.Nil(t, err)
		if completed {
			break
		}
		handler(h, s, v)
	}

	scanners[0].Close()
}

func simpleFullScan(t *testing.T, tb TableConnector, handler func(hashKey, sortKey, value []byte)) {
	simpleFullScanOpts(t, tb, handler, NewScanOptions())
}

// set a bunch of data into Pegasus
// `allData` is a map of hashkey->sortkey->value
func setDatabase(t *testing.T, tb TableConnector, allData map[string]map[string]string) {
	for i := 0; i < 100; i++ {
		hashKey := fmt.Sprintf("%d", i)
		sortMap, ok := allData[hashKey]
		if !ok {
			allData[hashKey] = map[string]string{}
			sortMap = allData[hashKey]
		}
		for j := 0; j < 10; j++ {
			sortKey := fmt.Sprintf("%d", j)
			value := "hello world"

			err := tb.Set(context.Background(), []byte(hashKey), []byte(sortKey), []byte(value))
			assert.Nil(t, err)
			sortMap[sortKey] = value
		}
	}
}

func TestPegasusTableConnector_ConcurrentCallScanner(t *testing.T) {
	defer leaktest.Check(t)()

	client := NewClient(testingCfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()

	baseMap := make(map[string]map[string]string)
	setDatabase(t, tb, baseMap)

	batchSizes := []int{5, 10, 100}
	var wg sync.WaitGroup
	for i := 0; i < len(batchSizes); i++ {
		wg.Add(1)

		go func(i int) {
			batchSize := batchSizes[i]
			options := NewScanOptions()
			options.BatchSize = batchSize

			var dataMap map[string]map[string]string
			simpleFullScanOpts(t, tb, func(hashKey, sortKey, value []byte) {
				if _, ok := dataMap[string(hashKey)]; !ok {
					dataMap[string(hashKey)] = map[string]string{}
				}
				dataMap[string(hashKey)][string(sortKey)] = string(value)
			}, options)

			assert.EqualValues(t, baseMap, dataMap)

			wg.Done()
		}(i)
	}
	wg.Wait()

	clearDatabase(t, tb)
}

func TestPegasusTableConnector_NoValueScan(t *testing.T) {
	defer leaktest.Check(t)()

	client := NewClient(testingCfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()

	baseMap := make(map[string]map[string]string)
	setDatabase(t, tb, baseMap)

	options := NewScanOptions()
	options.NoValue = true
	simpleFullScanOpts(t, tb, func(hashKey, sortKey, value []byte) {
		assert.Empty(t, value)
	}, options)

	clearDatabase(t, tb)
}

func listSortKeysFrom(t *testing.T, tb TableConnector, hashKey []byte, startSortKey, stopSortKey []byte, opts *ScannerOptions) []string {
	scanner, err := tb.GetScanner(context.Background(), []byte("h1"), startSortKey, stopSortKey, opts)
	assert.Nil(t, err)
	assert.NotNil(t, scanner)

	var sortKeys []string
	for {
		completed, h, s, _, err := scanner.Next(context.Background())
		assert.Nil(t, err)
		if completed {
			break
		}
		assert.Equal(t, h, hashKey)
		sortKeys = append(sortKeys, string(s))
	}
	return sortKeys
}

func TestPegasusTableConnector_ScanInclusive(t *testing.T) {
	defer leaktest.Check(t)()

	client := NewClient(testingCfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()

	for i := 0; i < 10; i++ {
		err := tb.Set(context.Background(), []byte("h1"), []byte(fmt.Sprint(i)), []byte("hello world"))
		assert.Nil(t, err)
	}

	opts := NewScanOptions()
	opts.StartInclusive = true
	sortKeys := listSortKeysFrom(t, tb, []byte("h1"), []byte("3"), nil, opts)
	assert.Equal(t, sortKeys[0], "3")

	opts.StartInclusive = false
	sortKeys = listSortKeysFrom(t, tb, []byte("h1"), []byte("3"), nil, opts)
	assert.Equal(t, sortKeys[0], []byte("4"))

	opts.StopInclusive = true
	sortKeys = listSortKeysFrom(t, tb, []byte("h1"), nil, []byte("6"), opts)
	assert.Equal(t, sortKeys[len(sortKeys)-1], []byte("6"))

	opts.StopInclusive = false
	sortKeys = listSortKeysFrom(t, tb, []byte("h1"), nil, []byte("6"), opts)
	assert.Equal(t, sortKeys[len(sortKeys)-1], []byte("5"))

	opts.StartInclusive = false
	opts.StopInclusive = false
	sortKeys = listSortKeysFrom(t, tb, []byte("h1"), []byte("6"), []byte("6"), opts)
	assert.Empty(t, sortKeys)

	opts.StartInclusive = true
	opts.StopInclusive = false
	sortKeys = listSortKeysFrom(t, tb, []byte("h1"), []byte("6"), []byte("6"), opts)
	assert.Equal(t, sortKeys[0], []byte("6"))

	clearDatabase(t, tb)
}

func TestPegasusTableConnector_ScanWithFilter(t *testing.T) {
	defer leaktest.Check(t)()

	client := NewClient(testingCfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()

	var start int64 = 1611331200 // 2021-01-23 00:00:00
	var end int64 = 1611676800   // 2021-01-27 00:00:00
	// Insert each minute timeString into DB
	for timeStamp := start; timeStamp < end; timeStamp += 60 {
		timeNow := time.Unix(timeStamp, 0)
		timeString := timeNow.Format("2006-01-02 15:04:05")
		tb.Set(context.Background(), []byte(timeString), []byte("cu"), []byte("fortest"))
	}

	sopts := &ScannerOptions{
		HashKeyFilter: Filter{Type: FilterTypeMatchAnywhere, Pattern: []byte("2021-01-25")},
	}
	minutePerDay := 0
	simpleFullScanOpts(t, tb, func(hashKey, sortKey, value []byte) {
		minutePerDay++
	}, sopts)

	assert.Equal(t, minutePerDay, 1440)

	clearDatabase(t, tb)
}
