// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package rpc

import (
	"io"
	"bufio"
)

const (
	readStreamBufferSize = 1024 * 64
)

// low-level rpc reader.
type ReadStream struct {
	bufReader *bufio.Reader
}

func (r *ReadStream) Next(toRead int) ([]byte, error) {
	buf := make([]byte, toRead)
	var total = 0

	readSz, err := r.bufReader.Read(buf)
	total += readSz
	for total < toRead && err == nil {
		readSz, err = r.bufReader.Read(buf[total:])
		total += readSz
	}

	if err != nil {
		return nil, err
	}
	return buf, nil
}

func NewReadStream(reader io.Reader) *ReadStream {
	return &ReadStream{
		bufReader: bufio.NewReaderSize(reader, readStreamBufferSize),
	}
}
