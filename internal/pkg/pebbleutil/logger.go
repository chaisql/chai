package pebbleutil

import "context"

// NoopLoggerAndTracer does no logging and tracing.
type NoopLoggerAndTracer struct{}

// Infof implements LoggerAndTracer.
func (l NoopLoggerAndTracer) Infof(format string, args ...interface{}) {}

// Errorf implements LoggerAndTracer.
func (l NoopLoggerAndTracer) Errorf(format string, args ...interface{}) {}

// Fatalf implements LoggerAndTracer.
func (l NoopLoggerAndTracer) Fatalf(format string, args ...interface{}) {}

// Eventf implements LoggerAndTracer.
func (l NoopLoggerAndTracer) Eventf(ctx context.Context, format string, args ...interface{}) {
}

// IsTracingEnabled implements LoggerAndTracer.
func (l NoopLoggerAndTracer) IsTracingEnabled(ctx context.Context) bool {
	return false
}
