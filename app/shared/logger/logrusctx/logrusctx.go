package logrusctx

import (
	"context"

	log "github.com/sirupsen/logrus"
)

type key int

var loggerKey key

func NewContext(ctx context.Context, contextLogger *log.Entry) context.Context {
	return context.WithValue(ctx, loggerKey, contextLogger)
}

func FromContext(ctx context.Context) (*log.Entry, bool) {
	l, ok := ctx.Value(loggerKey).(*log.Entry)
	return l, ok
}

func MustFromContext(ctx context.Context) *log.Entry {
	l, ok := ctx.Value(loggerKey).(*log.Entry)
	if !ok {
		//panic("could not find logrus.Entry from context")
		l = log.NewEntry(log.StandardLogger())
	}
	return l
}
