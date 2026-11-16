// Package serde defines the codec the engine uses for every value it
// persists: workflow inputs, workflow outputs, step outputs, and checkpoint
// payloads.
package serde

import "encoding/json"

// Codec encodes and decodes the values a workflow persists.
type Codec interface {
	Encode(v any) ([]byte, error)
	Decode(data []byte, v any) error
}

// JSON is the default Codec. Values must be JSON-encodable, which is the same
// constraint a typical HTTP API already imposes.
var JSON Codec = jsonCodec{}

type jsonCodec struct{}

func (jsonCodec) Encode(v any) ([]byte, error) { return json.Marshal(v) }

func (jsonCodec) Decode(data []byte, v any) error { return json.Unmarshal(data, v) }
