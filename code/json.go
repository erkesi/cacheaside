package code

import (
	"encoding/json"
)

type Json struct {
}

func (j *Json) Encode(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (j *Json) Decode(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

func (j *Json) Name() string {
	return "json"
}
