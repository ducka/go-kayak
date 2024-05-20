package utils

import (
	"reflect"
)

func Coalesce(values ...interface{}) interface{} {
	for _, v := range values {
		if v != nil {
			if reflect.TypeOf(v).Kind() == reflect.Ptr {
				if reflect.ValueOf(v).IsNil() {
					continue
				}
			}
			return v
		}
	}
	return nil
}
