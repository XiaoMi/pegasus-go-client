package pegasus

import (
	"context"
	"fmt"
	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/XiaoMi/pegasus-go-client/idl/rrdb"
	"strconv"
	"time"
)

type ScannerOptions struct {
	Timeout time.Duration

	BatchSize      int
	StartInclusive bool
	StopInclusive  bool
	HashKeyFilter  Filter
	SortKeyFilter  Filter
	noValue        bool
}

const (
	ContextIdValidMin  = 0
	ContextIdCompleted = -1
	ContextIdNotExist  = -2
)

type Scanner interface {
	Next(ctx context.Context) (err error, hashKey []byte, sortKey []byte, value []byte)
	Close(ctx context.Context)
}

type pegasusScanner struct {
	table          TableConnector
	hashKey        *base.Blob
	startKey       *base.Blob
	stopKey        *base.Blob
	splitGpid      []*base.Gpid
	options        ScannerOptions
	gpid           *base.Gpid
	kvs            []*KeyValue
	kvsIndex       int
	hashIndex      int //index of splitGpid[]
	idStatus       int64
	nextRunning    chan bool
	encounterError bool
	closed         bool
}

func NewScanOptions() *ScannerOptions {
	return &ScannerOptions{
		BatchSize:      1000,
		StartInclusive: true,
		StopInclusive:  false,
		HashKeyFilter:  Filter{Type: FilterTypeNoFilter, Pattern: nil},
		SortKeyFilter:  Filter{Type: FilterTypeNoFilter, Pattern: nil},
		noValue:        false,
	}
}

func NewPegasusScanner(table TableConnector, splitHash []*base.Gpid, options ScannerOptions, startKey *base.Blob,
	stopKey *base.Blob) Scanner {
	var splitGpid []*base.Gpid
	if len(splitHash) == 0 {
		splitGpid = make([]*base.Gpid, 0)
	} else {
		splitGpid = splitHash
	}

	return &pegasusScanner{
		table:          table,
		splitGpid:      splitGpid,
		options:        options,
		startKey:       startKey,
		stopKey:        stopKey,
		kvsIndex:       -1,
		idStatus:       ContextIdCompleted,
		hashIndex:      len(splitGpid),
		kvs:            make([]*KeyValue, 0),
		nextRunning:    make(chan bool, 1),
		encounterError: false,
		closed:         false,
	}
}

func NewPegasusScannerForUnorderedScanners(table TableConnector, splitHash []*base.Gpid, options ScannerOptions) Scanner {
	options.StartInclusive = true
	options.StopInclusive = false
	return NewPegasusScanner(table, splitHash, options, &base.Blob{Data: []byte{0x00, 0x00}}, &base.Blob{Data: []byte{0xFF, 0xFF}})
}

func (p *pegasusScanner) Next(ctx context.Context) (error, []byte, []byte, []byte) {
	err, h, s, v := func() (error, []byte, []byte, []byte) {
		if p.encounterError {
			return fmt.Errorf("last Next() failed"), nil, nil, nil
		}
		if p.closed {
			return fmt.Errorf("scanner is closed"), nil, nil, nil
		}
		p.nextRunning <- true
		defer func() {
			<-p.nextRunning
		}()
		return p.doNext(ctx)
	}()
	return wrapError(err, OpScan), h, s, v
}

func (p *pegasusScanner) doNext(ctx context.Context) (error, []byte, []byte, []byte) {
	for p.kvsIndex++; p.kvsIndex >= len(p.kvs); p.kvsIndex++ {
		if p.idStatus == ContextIdCompleted {
			if p.hashIndex <= 0 {
				return fmt.Errorf("no more hashIndex"),
					nil, nil, nil
			} else {
				p.hashIndex--
				p.gpid = p.splitGpid[p.hashIndex]
				p.splitReset()
			}
		} else if p.idStatus == ContextIdNotExist {
			return p.startScan(ctx)
		} else {
			return p.nextBatch(ctx)
		}
	}
	//kvs.SortKey=<hashKey,sortKey>
	err, h, s := restoreKey(p.kvs[p.kvsIndex].SortKey)
	return err, h, s, p.kvs[p.kvsIndex].Value
}

func (p *pegasusScanner) splitReset() {
	p.kvs = make([]*KeyValue, 0)
	p.kvsIndex = -1
	p.idStatus = ContextIdNotExist
}

func (p *pegasusScanner) startScan(ctx context.Context) (error, []byte, []byte, []byte) {
	request := rrdb.NewGetScannerRequest()
	if len(p.kvs) == 0 {
		request.StartKey = p.startKey
		request.StartInclusive = p.options.StartInclusive
	} else {
		request.StartKey = &base.Blob{Data: p.kvs[len(p.kvs)-1].SortKey}
		request.StartInclusive = false
	}
	request.StopKey = p.stopKey
	request.StopInclusive = p.options.StopInclusive
	request.BatchSize = int32(p.options.BatchSize)
	request.NoValue = p.options.noValue
	request.HashKeyFilterType = rrdb.FilterType(p.options.HashKeyFilter.Type)
	if p.options.HashKeyFilter.Pattern == nil {
		request.HashKeyFilterPattern = &base.Blob{} //the *base.Blob can't be nil, member Data is nil
	} else {
		request.HashKeyFilterPattern = &base.Blob{Data: p.options.HashKeyFilter.Pattern}
	}
	request.SortKeyFilterType = rrdb.FilterType(p.options.SortKeyFilter.Type)
	if p.options.SortKeyFilter.Pattern == nil {
		request.SortKeyFilterPattern = &base.Blob{}
	} else {
		request.SortKeyFilterPattern = &base.Blob{Data: p.options.SortKeyFilter.Pattern}
	}

	part := getPart(p.table.(*pegasusTableConnector), p.gpid)
	response, err := part.GetScanner(ctx, p.splitGpid[p.hashIndex], request)

	err = p.onRecvRpcResponse(response, err)
	if err == nil {
		return p.doNext(ctx)
	}

	return err, nil, nil, nil
}

func (p *pegasusScanner) nextBatch(ctx context.Context) (error, []byte, []byte, []byte) {
	request := &rrdb.ScanRequest{ContextID: p.idStatus}
	part := getPart(p.table.(*pegasusTableConnector), p.gpid)
	response, err := part.Scan(ctx, p.gpid, request)
	err = p.onRecvRpcResponse(response, err)
	if err == nil {
		return p.doNext(ctx)
	}

	p.encounterError = true
	return err, nil, nil, nil
}

func (p *pegasusScanner) onRecvRpcResponse(response *rrdb.ScanResponse, err error) error {
	if err == nil {
		if response.Error == 0 {
			// ERR_OK
			if len(response.Kvs) != 0 {
				p.kvs = make([]*KeyValue, len(response.Kvs))
				for i := 0; i < len(response.Kvs); i++ {
					p.kvs[i] = &KeyValue{
						SortKey: response.Kvs[i].Key.Data, //kvs.SortKey=<hashKey,sortKey>
						Value:   response.Kvs[i].Value.Data,
					}
				}
			}
			p.kvsIndex = -1
			p.idStatus = response.ContextID
		} else if response.Error == 1 {
			// rocksDB error kNotFound, that scan context has been removed
			p.idStatus = ContextIdNotExist
		} else {
			// rpc succeed, but operation encounter some error in server side
			return fmt.Errorf("rocksDB error:" + strconv.Itoa(int(response.Error)))
		}
	} else {
		// rpc failed
		return fmt.Errorf("scan failed with error:" + err.Error())
	}

	return nil
}

func (p *pegasusScanner) Close(ctx context.Context) {
	if p.idStatus >= ContextIdValidMin {
		part := getPart(p.table.(*pegasusTableConnector), p.gpid)
		err := part.ClearScanner(ctx, p.gpid, p.idStatus)
		if err == nil {
			p.idStatus = ContextIdCompleted
		}
	}
	p.hashIndex = 0
	p.closed = true
}
