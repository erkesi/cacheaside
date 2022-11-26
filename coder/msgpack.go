package coder

import (
	msgpack "github.com/vmihailenco/msgpack/v5"
)

type MsgPack struct {
}

func (j *MsgPack) Encode(v interface{}) ([]byte, error) {
	return msgpack.Marshal(v)
}

func (j *MsgPack) Decode(data []byte, v interface{}) error {
	return msgpack.Unmarshal(data, v)
}

func (j *MsgPack) Name() string {
	return "json"
}
