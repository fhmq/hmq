/* Copyright (c) 2018, joy.zhou <chowyu08@gmail.com>
 */

package logger

import (
	"go.uber.org/zap"
)

var (
	// env can be setup at build time with Go Linker. Value could be prod or whatever else for dev env
	instance *zap.Logger
	logCfg   zap.Config
)

// NewDevLogger return a logger for dev builds
func NewDevLogger() (*zap.Logger, error) {
	logCfg := zap.NewDevelopmentConfig()
	// logCfg.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	logCfg.DisableStacktrace = true
	return logCfg.Build()
}

// NewProdLogger return a logger for production builds
func NewProdLogger() (*zap.Logger, error) {
	logCfg := zap.NewProductionConfig()
	logCfg.DisableStacktrace = true
	logCfg.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	return logCfg.Build()
}

func InitLogger(debug bool) {
	var err error
	var log *zap.Logger
	if debug {
		log, err = NewDevLogger()
	} else {
		log, err = NewProdLogger()
	}
	if err != nil {
		panic("Unable to create a logger.")
	}
	defer log.Sync()

	log.Debug("Logger initialization succeeded")
	instance = log.Named("hmq")
}

// Get return a *zap.Logger instance
func Get() *zap.Logger {
	if instance == nil {
		l, _ := NewProdLogger()
		return l
	}
	return instance
}
