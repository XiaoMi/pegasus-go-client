// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package pegasus

// Config is the configuration of pegasus client.
type Config struct {
	MetaServers []string `json:"meta_servers"`
}

var testingCfg = Config{
	MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
}
