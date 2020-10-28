package admin

import (
	"context"

	"github.com/XiaoMi/pegasus-go-client/session"
)

// Client provides the administration API to a specific cluster.
// Remember only the superusers configured to the cluster have the admin priviledges.
type Client interface {
	CreateTable(ctx context.Context, tableName string, partitionCount int) error

	DropTable(ctx context.Context, tableName string) error
}

type Config struct {
	MetaServers []string `json:"meta_servers"`
}

// NewClient returns an instance of Client.
func NewClient(cfg Config) Client {
	return &rpcBasedClient{
		metaManager: session.NewMetaManager(cfg.MetaServers, session.NewNodeSession),
	}
}

type rpcBasedClient struct {
	metaManager *session.MetaManager
}

func (c *rpcBasedClient) CreateTable(ctx context.Context, tableName string, partitionCount int) error {
	return c.metaManager.CreateTable(ctx, tableName, partitionCount)
}

func (c *rpcBasedClient) DropTable(ctx context.Context, tableName string) error {
	return c.metaManager.DropTable(ctx, tableName)
}
