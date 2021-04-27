package base

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRPCAddress(t *testing.T) {
	testCases := []string{
		"127.0.0.1:8080",
		"192.168.0.1:123",
		"0.0.0.0:12345",
	}

	for _, ts := range testCases {
		tcpAddrStr := ts
		addr, err := net.ResolveTCPAddr("tcp", tcpAddrStr)
		assert.NoError(t, err)

		rpcAddr := NewRPCAddress(addr.IP, addr.Port)
		assert.Equal(t, rpcAddr.GetAddress(), tcpAddrStr)
	}
}
