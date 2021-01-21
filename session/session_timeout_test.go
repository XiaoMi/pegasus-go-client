package session

import (
	"bytes"
	"github.com/XiaoMi/pegasus-go-client/rpc"
	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
	"io"
	"net"
	"testing"
)

type timeoutReader struct {
	readTimes int
}

type timeoutError struct {
	*net.OpError
}

func (*timeoutError) Timeout() bool {
	return true
}

func (*timeoutError) Error() string {
	return "i/o timeout"
}

func (r *timeoutReader) Read([]byte) (n int, err error) {
	if r.readTimes == 0 {
		r.readTimes++
		return 0, &net.OpError{Err: &timeoutError{}}
	}
	return 0, io.EOF
}

func TestNodeSession_ReadTimeout(t *testing.T) {
	defer leaktest.Check(t)()

	reader := timeoutReader{}
	_, err := reader.Read(nil)
	assert.True(t, rpc.IsNetworkTimeoutErr(err))

	idleStateHandlerCalled := false
	n := newFakeNodeSession(&timeoutReader{}, bytes.NewBuffer(make([]byte, 0)))
	n.idleStateHandler = func(s NodeSession) {
		idleStateHandlerCalled = true
	}

	err = n.loopForResponse() // since the timeoutReader returns EOF at last, the loop will finally terminate
	assert.Nil(t, err)
	assert.True(t, idleStateHandlerCalled)
}
