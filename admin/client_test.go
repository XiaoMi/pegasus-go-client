package admin

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAdmin_CreateTable(t *testing.T) {
	c := NewClient(Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	})
	err := c.CreateTable(context.Background(), "temp", 16)
	assert.Nil(t, err)
}
