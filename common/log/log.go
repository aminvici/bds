package log

import (
	"fmt"
	jlog "github.com/aminvici/bds/common/jlog"
)

var (
	NormalLogger = jlog.GetLogger("normal")
	AccessLogger = jlog.GetLogger("access")
	DetailLogger = jlog.GetLogger("detail")
)

func Error(content string, args ...interface{}) {
	NormalLogger.Error(content, args...)
}

func Info(content string, args ...interface{}) {
	NormalLogger.Info(content, args...)
}

func Debug(content string, args ...interface{}) {
	NormalLogger.Debug(content, args...)
}

func Warn(content string, args ...interface{}) {
	NormalLogger.Warn(content, args...)
}

func Fatal(content string, args ...interface{}) {
	NormalLogger.Fatal(content, args...)
}

func AccessLog(content string, args ...interface{}) {
	AccessLogger.Info(content, args...)
}

func DetailError(content interface{}, args ...interface{}) {
	DetailLogger.Error(content, args...)
}

func DetailDebug(content interface{}, args ...interface{}) {
	DetailLogger.Debug(content, args...)
}

type Logger struct {
	requestID string
}

func NewLogger() *Logger {
	return new(Logger)
}

func (l *Logger) SetRequestID(requestID string) *Logger {
	l.requestID = requestID
	return l
}

func (l *Logger) GetRequestID() string {
	return l.requestID
}

func (l *Logger) Error(content string, args ...interface{}) {
	if len(l.requestID) != 0 {
		NormalLogger.Error("%s %s", l.requestID, fmt.Sprintf(content, args...))
	} else {
		NormalLogger.Error(content, args...)
	}
}

func (l *Logger) DetailError(content string, args ...interface{}) {
	if len(l.requestID) != 0 {
		DetailLogger.Error("%s %s", l.requestID, fmt.Sprintf(content, args...))
	} else {
		DetailLogger.Error(content, args...)
	}
}

func (l *Logger) Info(content string, args ...interface{}) {
	if len(l.requestID) != 0 {
		NormalLogger.Info("%s %s", l.requestID, fmt.Sprintf(content, args...))
	} else {
		NormalLogger.Info(content, args...)
	}
}

func (l *Logger) Debug(content string, args ...interface{}) {
	if len(l.requestID) != 0 {
		NormalLogger.Debug("%s %s", l.requestID, fmt.Sprintf(content, args...))
	} else {
		NormalLogger.Debug(content, args...)
	}
}

func (l *Logger) DetailDebug(content string, args ...interface{}) {
	if len(l.requestID) != 0 {
		DetailLogger.Debug("%s %s", l.requestID, fmt.Sprintf(content, args...))
	} else {
		DetailLogger.Debug(content, args...)
	}
}

func (l *Logger) Warn(content string, args ...interface{}) {
	if len(l.requestID) != 0 {
		NormalLogger.Warn("%s %s", l.requestID, fmt.Sprintf(content, args...))
	} else {
		NormalLogger.Warn(content, args...)
	}
}

func (l *Logger) Fatal(content string, args ...interface{}) {
	if len(l.requestID) != 0 {
		NormalLogger.Fatal("%s %s", l.requestID, fmt.Sprintf(content, args...))
	} else {
		NormalLogger.Fatal(content, args...)
	}
}
