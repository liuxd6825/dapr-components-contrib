package logs

import "context"

type Logger interface {
	Info(args ...interface{})
	Infof(format string, args ...interface{})
	Debug(args ...interface{})
	Debugf(format string, args ...interface{})
	Warn(args ...interface{})
	Warnf(format string, args ...interface{})
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
}

func GetLogger(ctx context.Context) Logger {
	if v := ctx.Value(loggerKey{}); v != nil {
		if log, ok := v.(Logger); ok {
			return log
		}
	}
	return nil
}

func Debug(ctx context.Context, args ...interface{}) {
	if l := GetLogger(ctx); l != nil {
		l.Debug(args...)
	}
}

func Info(ctx context.Context, args ...interface{}) {
	if l := GetLogger(ctx); l != nil {
		l.Info(args...)
	}
}

func Warn(ctx context.Context, args ...interface{}) {
	if l := GetLogger(ctx); l != nil {
		l.Warn(args...)
	}
}

func Error(ctx context.Context, args ...interface{}) {
	if l := GetLogger(ctx); l != nil {
		l.Error(args...)
	}
}

func Fatal(ctx context.Context, args ...interface{}) {
	if l := GetLogger(ctx); l != nil {
		l.Fatal(args...)
	}
}

func Debugf(ctx context.Context, format string, args ...interface{}) {
	if l := GetLogger(ctx); l != nil {
		l.Debugf(format, args...)
	}
}

func Infof(ctx context.Context, format string, args ...interface{}) {
	if l := GetLogger(ctx); l != nil {
		l.Infof(format, args...)
	}
}

func Warnf(ctx context.Context, format string, args ...interface{}) {
	if l := GetLogger(ctx); l != nil {
		l.Warnf(format, args...)
	}
}

func Errorf(ctx context.Context, format string, args ...interface{}) {
	if l := GetLogger(ctx); l != nil {
		l.Errorf(format, args...)
	}
}

func Fatalf(ctx context.Context, format string, args ...interface{}) {
	if l := GetLogger(ctx); l != nil {
		l.Fatalf(format, args...)
	}
}
