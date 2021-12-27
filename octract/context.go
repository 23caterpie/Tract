package octract

import (
	"context"

	tract "github.com/23caterpie/Tract"

	"go.opencensus.io/trace"
)

func getCtx(req tract.Request) (context.Context, func(context.Context)) {
	switch req := req.(type) {
	case tract.ContextRequest:
		return req.Context()
	}
	return context.Background(), func(context.Context) {}
}

// Only do tracing if we're dealing with a tract.ContextRequest. Would have diconnected spans otherwise.
func startSpan(req tract.Request, spanName string) func() {
	ctxRequest, ok := req.(tract.ContextRequest)
	if !ok {
		return func() {}
	}

	ctx, setCtx := ctxRequest.Context()
	ctx, span := trace.StartSpan(ctx, spanName)
	setCtx(ctx)
	return span.End
}
