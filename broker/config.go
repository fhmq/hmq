package broker

import (
	"encoding/json"
	"errors"
	"io/ioutil"

	"github.com/prometheus/common/log"
)

const (
	CONFIGFILE = "broker.config"
)

type Config struct {
	Host string `json:"host"`
	Port string `json:"port"`
}

func LoadConfig() (*Config, error) {
	content, err := ioutil.ReadFile(CONFIGFILE)
	if err != nil {
		log.Error("Read config file error: ", err)
		return nil, err
	}
	var info Config
	err = json.Unmarshal(content, &info)
	if err != nil {
		log.Error("Unmarshal config file error: ", err)
		return nil, err
	}

	if info.Port != "" {
		if info.Host == "" {
			info.Host = "0.0.0.0"
		}
	} else {
		return nil, errors.New("Listen port nil")
	}

	return &info, nil
}
