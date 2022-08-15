package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/uptrace/bun"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type QueryHook struct {
	errorFieldName  string
	precision       time.Duration
	logger          *zap.Logger
	enabled         bool
	verbose         bool
	durationAsField bool
	errorAsField    bool
	duration        bool
	queryLevel      zapcore.Level
	errorLevel      zapcore.Level
}

type Option func(*QueryHook)

// WithEnabled enables/disables the hook.
func WithEnabled(on bool) Option {
	return func(h *QueryHook) {
		h.enabled = on
	}
}

// WithVerbose configures the hook to log all queries
// (by default, only failed queries are logged).
func WithVerbose(on bool) Option {
	return func(h *QueryHook) {
		h.verbose = on
	}
}

// WithDurationAsField configures the hook to set the duration as field,
// written in the message by default.
func WithDurationAsField() Option {
	return func(h *QueryHook) {
		h.duration = true
		h.durationAsField = true
	}
}

// WithDurationPrecision configures the hook to log the duration with
// the specified precision.
// e.g. passing time.Millisecond returns a duration in ms.
func WithDurationPrecision(precision time.Duration) Option {
	return func(h *QueryHook) {
		h.durationAsField = true
	}
}

// WithErrorAsField configures the hook to log the error as a field.
func WithErrorAsField(field string) Option {
	return func(h *QueryHook) {
		h.errorAsField = true
		h.errorFieldName = field
	}
}

// WithLevels configures the hook to make proper usage of zap levels.
func WithLevels(queryLevel, errorLevel zapcore.Level) Option {
	return func(h *QueryHook) {
		h.queryLevel = queryLevel
		h.errorLevel = errorLevel
	}
}

// WithDuration configures the hook to log the duration.
func WithDuration() Option {
	return func(h *QueryHook) {
		h.duration = true
	}
}

// NewQueryHook creates a new query hook.
func NewQueryHook(logger *zap.Logger, opts ...Option) *QueryHook {
	qh := &QueryHook{
		errorFieldName:  "error",
		precision:       time.Millisecond,
		logger:          logger,
		enabled:         true,
		verbose:         false,
		durationAsField: false,
		errorAsField:    false,
		duration:        false,
		queryLevel:      zapcore.DebugLevel,
		errorLevel:      zapcore.ErrorLevel,
	}

	for _, opt := range opts {
		opt(qh)
	}

	return qh
}

func (h *QueryHook) BeforeQuery(ctx context.Context, _ *bun.QueryEvent) context.Context { return ctx }

func (h *QueryHook) AfterQuery(_ context.Context, event *bun.QueryEvent) {
	if !h.enabled {
		return
	}

	var level zapcore.Level
	var err error

	switch event.Err {
	case nil, sql.ErrNoRows, sql.ErrTxDone:
		if !h.verbose {
			return
		}
		level = h.queryLevel
		err = nil
	default:
		level = h.errorLevel
		err = event.Err
	}

	now := time.Now()
	dur := now.Sub(event.StartTime)

	message := event.Query
	fields := []zap.Field{}

	if h.duration && h.durationAsField {
		fields = append(fields, zap.Field{
			Key:       "duration",
			Type:      zapcore.StringerType,
			Interface: dur.Round(h.precision),
		})
	} else if h.duration {
		message = fmt.Sprintf("duration: %s %s", dur.Round(h.precision), message)
	}

	if err != nil {
		if h.errorAsField {
			fields = append(fields, zap.Field{
				Key:       h.errorFieldName,
				Type:      zapcore.ErrorType,
				Interface: err,
			})
		} else {
			message = fmt.Sprintf("%s error: %s", message, err)
		}
	}

	h.logger.Log(level, message, fields...)
}
