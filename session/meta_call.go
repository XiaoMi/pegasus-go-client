package session

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/base"
)

type metaCallFunc func(context.Context, *metaSession) (metaResponse, error)

type metaResponse interface {
	GetErr() *base.ErrorCode
}

type metaCall struct {
	respCh   chan metaResponse
	backupCh chan interface{}
	callFunc metaCallFunc

	metas   []*metaSession
	lead    int
	newLead uint32
}

func newMetaCall(lead int, metas []*metaSession, callFunc metaCallFunc) *metaCall {
	return &metaCall{
		metas:    metas,
		lead:     lead,
		newLead:  uint32(lead),
		respCh:   make(chan metaResponse),
		callFunc: callFunc,
		backupCh: make(chan interface{}),
	}
}

func (c *metaCall) Run(ctx context.Context) (metaResponse, error) {
	// the subroutines will be cancelled when this call ends
	subCtx, cancel := context.WithCancel(ctx)

	go func() {
		// issue RPC to leader
		if !c.issueSingleMeta(subCtx, c.lead) {
			select {
			case <-subCtx.Done():
			case c.backupCh <- nil:
				// after the leader failed, we immediately start another
				// RPC to the backup.
			}
		}
	}()

	go func() {
		// Automatically issue backup RPC after a period
		// when the current leader is suspected unvailable.
		ticker := time.NewTicker(1 * time.Second) // TODO(wutao): make it configurable
		select {
		case <-ticker.C:
			c.issueBackupMetas(subCtx)
		case <-c.backupCh:
			c.issueBackupMetas(subCtx)
		case <-subCtx.Done():
		}
	}()

	// The result of meta query is always a context error, or success.
	select {
	case resp := <-c.respCh:
		cancel()
		return resp, nil
	case <-ctx.Done():
		cancel()
		return nil, ctx.Err()
	}
}

// issueSingleMeta returns false if we should try another meta
func (c *metaCall) issueSingleMeta(ctx context.Context, i int) bool {
	meta := c.metas[i]
	resp, err := c.callFunc(ctx, meta)
	if err != nil || resp.GetErr().Errno == base.ERR_FORWARD_TO_OTHERS.String() {
		return false
	}
	select {
	case <-ctx.Done():
	case c.respCh <- resp:
		// the RPC succeeds, this meta becomes the new leader now.
		atomic.StoreUint32(&c.newLead, uint32(i))
	}
	return true
}

func (c *metaCall) issueBackupMetas(ctx context.Context) {
	for i := range c.metas {
		if i == c.lead {
			continue
		}
		// concurrently issue RPC to the rest of meta servers.
		go func(idx int) {
			c.issueSingleMeta(ctx, idx)
		}(i)
	}
}
