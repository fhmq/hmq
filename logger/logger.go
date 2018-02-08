/* Copyright (c) 2018, joy.zhou <chowyu08@gmail.com>
 */

package logger

import (
	"go.uber.org/zap"
)

var (
	logInstance *zap.Logger
)

// InitDevLogger instanciate a logger for dev builds
func InitDevLogger() {
	logCfg := zap.NewDevelopmentConfig()
	logInstance, _ = logCfg.Build()
}

// InitProdLogger instanciate a logger for production builds
func InitProdLogger() {
	logCfg := zap.NewProductionConfig()
	logCfg.DisableStacktrace = true
	logCfg.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	logInstance, _ = logCfg.Build()
}

func InitLogger(debug bool) {
	var err error
	if debug {
		InitDevLogger()
	} else {
		InitProdLogger()
	}
	if err != nil {
		panic("Unable to create a logger.")
	}
	logInstance.Debug("Logger initialization succeeded")
}

// Get the existing *zap.Logger instance. If none have been created, it'll instanciate de dev logger
func Get() *zap.Logger {
	if logInstance == nil {
		InitDevLogger()
	}
	return logInstance
}
