package logs

import (
	"context"
)

type loggerKey struct {
}

func NewContext(parentCtx context.Context, logger Logger) context.Context {
	ctx := context.WithValue(parentCtx, loggerKey{}, logger)
	return ctx
}
