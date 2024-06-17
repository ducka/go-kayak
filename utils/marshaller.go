package utils

import (
	"encoding/json"
)

type Marshaller interface {
	Serialize(inObj interface{}) (string, error)
	Deserialize(inJson string, outObj interface{}) error
}

type JsonMarshaller struct {
}

func NewJsonMarshaller() *JsonMarshaller {
	return &JsonMarshaller{}
}

func (j *JsonMarshaller) Serialize(t any) (string, error) {
	bytes, err := json.Marshal(t)
	return string(bytes), err
}

func (j *JsonMarshaller) Deserialize(s string, out interface{}) error {
	return json.Unmarshal([]byte(s), out)
}
