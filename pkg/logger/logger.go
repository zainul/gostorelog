package logger

import (
	"log"
	"os"
)

// Logger interface
type Logger interface {
	Info(msg string)
	Error(msg string)
}

// SimpleLogger implements Logger
type SimpleLogger struct {
	info  *log.Logger
	error *log.Logger
}

// NewSimpleLogger creates a new simple logger
func NewSimpleLogger() Logger {
	return &SimpleLogger{
		info:  log.New(os.Stdout, "INFO: ", log.LstdFlags),
		error: log.New(os.Stderr, "ERROR: ", log.LstdFlags),
	}
}

// Info logs info message
func (l *SimpleLogger) Info(msg string) {
	l.info.Println(msg)
}

// Error logs error message
func (l *SimpleLogger) Error(msg string) {
	l.error.Println(msg)
}