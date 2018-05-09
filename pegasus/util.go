// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package pegasus

import (
	"encoding/binary"
	"hash/crc64"

	"github.com/XiaoMi/pegasus-go-client/idl/base"
)

func encodeHashKeySortKey(hashKey []byte, sortKey []byte) *base.Blob {
	hashKeyLen := len(hashKey)
	sortKeyLen := len(sortKey)

	blob := &base.Blob{
		Data: make([]byte, 2+hashKeyLen+sortKeyLen),
	}

	binary.BigEndian.PutUint16(blob.Data, uint16(hashKeyLen))

	if hashKeyLen > 0 {
		copy(blob.Data[2:], hashKey)
	}

	if sortKeyLen > 0 {
		copy(blob.Data[2+hashKeyLen:], sortKey)
	}

	return blob
}

func encodeNextBytes(hashKey []byte) *base.Blob {
	hashKeyLen := len(hashKey)

	blob := &base.Blob{
		Data: make([]byte, 2+hashKeyLen),
	}

	binary.BigEndian.PutUint16(blob.Data, uint16(hashKeyLen))

	if hashKeyLen > 0 {
		copy(blob.Data[2:], hashKey)
	}

	i := len(blob.Data) - 1
	for ; i >= 0; i-- {
		if blob.Data[i] != 0xFF {
			blob.Data[i]++
			break
		}
	}

	return &base.Blob{Data: blob.Data[:i+1]}

}

func encodeNextBytesByPattern(hashKey []byte, sortKey []byte) *base.Blob {
	key := encodeHashKeySortKey(hashKey, sortKey)
	array := key.Data

	i := len(array) - 1
	for ; i >= 0; i-- {
		if array[i] != 0xFF {
			array[i]++
			break
		}
	}
	return &base.Blob{Data: array[:i+1]}
}

var crc64Table = crc64.MakeTable(0x9a6c9329ac4bc9b5)

func crc64Hash(data []byte) uint64 {
	return crc64.Checksum(data, crc64Table)
}
