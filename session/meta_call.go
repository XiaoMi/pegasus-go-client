package session

import (
	"context"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/base"
)

type metaCallFunc func(context.Context, *metaSession) (metaResponse, error)

type metaResponse interface {
	GetErr() *base.ErrorCode
}

type metaCall struct {
	respCh   chan metaResponse
	metas    []*metaSession
	lead     int
	callFunc metaCallFunc
	backupCh chan interface{}
}

func newMetaCall(lead int, metas []*metaSession, callFunc metaCallFunc) *metaCall {
	return &metaCall{
		metas:    metas,
		lead:     lead,
		respCh:   make(chan metaResponse),
		callFunc: callFunc,
		backupCh: make(chan interface{}),
	}
}

func (c *metaCall) Run(ctx context.Context) (metaResponse, error) {
	// the subroutines will be cancelled when this call ends
	subCtx, cancel := context.WithCancel(ctx)

	go func() {
		// issue to leader
		if !c.issueSingleMeta(subCtx, c.metas[c.lead]) {
			select {
			case <-subCtx.Done():
			case c.backupCh <- nil:
			}
		}
	}()

	go func() {
		// Automatically issue backup RPC after a period
		// when the current leader is suspected unvailable.
		ticker := time.NewTicker(1 * time.Second) // TODO(wutao): make it configurable
		select {
		case <-c.backupCh:
			c.issueBackupMetas(subCtx)
		case <-ticker.C:
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
func (c *metaCall) issueSingleMeta(ctx context.Context, meta *metaSession) bool {
	resp, err := c.callFunc(ctx, meta)
	if err != nil || resp.GetErr().Errno == base.ERR_FORWARD_TO_OTHERS.Error() {
		return false
	}
	select {
	case <-ctx.Done():
	case c.respCh <- resp:
	}
	return true
}

func (c *metaCall) issueBackupMetas(ctx context.Context) {
	for i, meta := range c.metas {
		if i == c.lead {
			continue
		}
		// concurrently issue RPC to the rest of meta servers.
		go func(meta *metaSession) {
			c.issueSingleMeta(ctx, meta)
		}(meta)
	}
}
