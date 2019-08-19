/* Copyright (c) 2018, joy.zhou <chowyu08@gmail.com>
 */

package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	// env can be setup at build time with Go Linker. Value could be prod or whatever else for dev env
	instance   *zap.Logger
	logCfg     zap.Config
	encoderCfg = zap.NewProductionEncoderConfig()
)

func init() {
	encoderCfg.TimeKey = "timestamp"
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder
}

// NewDevLogger return a logger for dev builds
func NewDevLogger() (*zap.Logger, error) {
	logCfg := zap.NewProductionConfig()
	logCfg.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	// logCfg.DisableStacktrace = true
	logCfg.EncoderConfig = encoderCfg
	return logCfg.Build()
}

// NewProdLogger return a logger for production builds
func NewProdLogger() (*zap.Logger, error) {
	logCfg := zap.NewProductionConfig()
	logCfg.DisableStacktrace = true
	logCfg.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	logCfg.EncoderConfig = encoderCfg
	return logCfg.Build()
}

func Prod() *zap.Logger {

	l, _ := NewProdLogger()
	instance = l

	return instance
}

func Debug() *zap.Logger {

	l, _ := NewDevLogger()
	instance = l

	return instance
}

func Get() *zap.Logger {
	if instance == nil {
		l, _ := NewProdLogger()
		instance = l
	}

	return instance
}
