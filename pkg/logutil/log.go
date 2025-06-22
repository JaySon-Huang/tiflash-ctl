package logutil

import (
	"context"
	"testing"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// BgLogger returns the default global logger.
func BgLogger() *zap.Logger {
	return log.L()
}

// Logger gets a contextual logger from current context.
// contextual logger will output common fields from context.
func Logger(ctx context.Context) *zap.Logger {
	if ctxlogger, ok := ctx.Value(CtxLogKey).(*zap.Logger); ok {
		return ctxlogger
	}
	return log.L()
}

type ctxLogKeyType struct{}

// CtxLogKey is the key to retrieve logger from context.
// It can be assigned to another value.
var CtxLogKey interface{} = ctxLogKeyType{}

// AssertWarn panics when in testing mode, and logs a warning msg otherwise.
func AssertWarn(logger *zap.Logger, msg string, fields ...zap.Field) {
	if testing.Testing() {
		logger.Panic(msg, fields...)
	}
	logger.Warn(msg, fields...)
}
