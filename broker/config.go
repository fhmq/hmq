package broker

import (
	"encoding/json"
	"errors"
	"io/ioutil"

	log "github.com/cihub/seelog"
)

const (
	CONFIGFILE = "broker.config"
)

type Config struct {
	Host    string    `json:"host"`
	Port    string    `json:"port"`
	Cluster RouteInfo `json:"cluster"`
}

type RouteInfo struct {
	Host   string   `json:"host"`
	Port   string   `json:"port"`
	Routes []string `json:"routes"`
}

func LoadConfig() (*Config, error) {
	content, err := ioutil.ReadFile(CONFIGFILE)
	if err != nil {
		log.Error("Read config file error: ", err)
		return nil, err
	}
	var config Config
	err = json.Unmarshal(content, &config)
	if err != nil {
		log.Error("Unmarshal config file error: ", err)
		return nil, err
	}

	if config.Port != "" {
		if config.Host == "" {
			config.Host = "0.0.0.0"
		}
	} else {
		return nil, errors.New("Listen port nil")
	}

	if config.Cluster.Port != "" {
		if config.Cluster.Host == "" {
			config.Cluster.Host = "0.0.0.0"
		}
	}

	return &config, nil
}
