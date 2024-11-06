package observe

import (
	"context"
)

type Context struct {
	context.Context
	Activity string
}

func NewContext(context context.Context, activity string) Context {
	return Context{
		Context:  context,
		Activity: activity,
	}
}
