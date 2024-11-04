package utils

import (
	"reflect"
)

func ValueOrFallback[T any](value *T, fallback T) T {
	if value == nil {
		return fallback
	}
	return *value
}

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

func ToPtr[T any](v T) *T {
	return &v
}

func ArrayToMap[K comparable, V any](items []V, keySelector func(V) K) map[K]V {
	result := make(map[K]V)
	for _, item := range items {
		result[keySelector(item)] = item
	}
	return result
}

func MapToArray[K comparable, V any](items map[K]V) []V {
	result := make([]V, 0, len(items))
	for _, item := range items {
		result = append(result, item)
	}
	return result
}
