/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package admin

import (
	"context"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/XiaoMi/pegasus-go-client/session"
)

// RemoteCmdClient is a client to call remote command to a Pegasus ReplicaServer.
type RemoteCmdClient struct {
	session session.NodeSession
}

// NewRemoteCmdClient returns an instance of RemoteCmdClient.
func NewRemoteCmdClient(addr string, nodeType session.NodeType) *RemoteCmdClient {
	return &RemoteCmdClient{
		session: session.NewNodeSession(addr, nodeType),
	}
}

func (c *RemoteCmdClient) Call(command string, arguments []string) (cmdResult string, err error) {
	ctx, cancelFn := context.WithTimeout(context.Background(), time.Second*5)
	thriftArgs := &RemoteCmdServiceCallCommandArgs{
		Cmd: &Command{Cmd: command, Arguments: arguments},
	}
	res, err := c.session.CallWithGpid(ctx, &base.Gpid{}, thriftArgs, "RPC_CLI_CLI_CALL")
	if err != nil {
		cancelFn()
		return "", err
	}
	ret, _ := res.(*RemoteCmdServiceCallCommandResult)
	cancelFn()
	return ret.GetSuccess(), nil
}
