package observe

import (
	"context"
)

type Context struct {
	context.Context
	Activity string
}
