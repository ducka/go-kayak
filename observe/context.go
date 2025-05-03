package observe

import (
	"context"
)

type Context struct {
	context.Context
	Activity   string
	key, value any
}

func (c Context) Value(key any) any {
	if key != nil && c.key == key {
		return c.value
	}
	return c.Context.Value(key)
}

func NewContext(context context.Context, activity string) Context {
	return Context{
		Context:  context,
		Activity: activity,
	}
}
func NewContextWithValue(parent Context, key, value any) Context {
	if key == nil {
		panic("nil key")
	}
	return Context{
		Context: parent,
		key:     key,
		value:   value,
	}
}
