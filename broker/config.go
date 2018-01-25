package broker

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"

	log "github.com/cihub/seelog"
)

type Config struct {
	Worker  int       `json:"workerNum"`
	Host    string    `json:"host"`
	Port    string    `json:"port"`
	Cluster RouteInfo `json:"cluster"`
	Router  string    `json:"router"`
	TlsHost string    `json:"tlsHost"`
	TlsPort string    `json:"tlsPort"`
	WsPath  string    `json:"wsPath"`
	WsPort  string    `json:"wsPort"`
	WsTLS   bool      `json:"wsTLS"`
	TlsInfo TLSInfo   `json:"tlsInfo"`
	Acl     bool      `json:"acl"`
	AclConf string    `json:"aclConf"`
}

type RouteInfo struct {
	Host string `json:"host"`
	Port string `json:"port"`
}

type TLSInfo struct {
	Verify   bool   `json:"verify"`
	CaFile   string `json:"caFile"`
	CertFile string `json:"certFile"`
	KeyFile  string `json:"keyFile"`
}

var DefaultConfig *Config = &Config{
	Worker: 4096,
	Host:   "0.0.0.0",
	Port:   "1883",
	Acl:    false,
}

func ConfigureConfig() (*Config, error) {
	config := &Config{}
	var (
		configFile string
	)
	flag.IntVar(&config.Worker, "w", 1024, "worker num to process message, perfer (client num)/10.")
	flag.IntVar(&config.Worker, "worker", 1024, "worker num to process message, perfer (client num)/10.")
	flag.StringVar(&config.Port, "port", "1883", "Port to listen on.")
	flag.StringVar(&config.Port, "p", "1883", "Port to listen on.")
	flag.StringVar(&config.Host, "host", "0.0.0.0", "Network host to listen on.")
	flag.StringVar(&config.Host, "h", "0.0.0.0", "Network host to listen on.")
	flag.StringVar(&config.Cluster.Host, "cluster", "", "Cluster ip from which members can connect.")
	flag.StringVar(&config.Cluster.Host, "cluster_listen", "", "Cluster ip from which members can connect.")
	flag.StringVar(&config.Cluster.Port, "cp", "", "Cluster port from which members can connect.")
	flag.StringVar(&config.Cluster.Port, "cluster_port", "", "Cluster port from which members can connect.")
	flag.StringVar(&config.Router, "r", "", "Router who maintenance cluster info")
	flag.StringVar(&config.Router, "router", "", "Router who maintenance cluster info")
	flag.StringVar(&config.WsPort, "wsport", "", "port for ws to listen on")
	flag.StringVar(&config.WsPort, "ws_port", "", "port for ws to listen on")
	flag.StringVar(&config.WsPath, "wspath", "", "path for ws to listen on")
	flag.StringVar(&config.WsPath, "ws_path", "", "path for ws to listen on")
	flag.StringVar(&configFile, "config", "", "config file for hmq")
	flag.StringVar(&configFile, "c", "", "config file for hmq")
	flag.Parse()

	if configFile != "" {
		tmpConfig, e := LoadConfig(configFile)
		if e != nil {
			return nil, e
		} else {
			config = tmpConfig
		}
	}

	if err := config.check(); err != nil {
		return nil, err
	}

	return config, nil

}

func LoadConfig(filename string) (*Config, error) {

	content, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Error("Read config file error: ", err)
		return nil, err
	}
	// log.Info(string(content))

	var config Config
	err = json.Unmarshal(content, &config)
	if err != nil {
		log.Error("Unmarshal config file error: ", err)
		return nil, err
	}

	return &config, nil
}

func (config *Config) check() error {

	if config.Worker == 0 {
		config.Worker = 1024
	}

	WorkNum = config.Worker

	if config.Port != "" {
		if config.Host == "" {
			config.Host = "0.0.0.0"
		}
	}

	if config.Cluster.Port != "" {
		if config.Cluster.Host == "" {
			config.Cluster.Host = "0.0.0.0"
		}
	}
	if config.Router != "" {
		if config.Cluster.Port == "" {
			return errors.New("cluster port is null")
		}
	}

	if config.TlsPort != "" {
		if config.TlsInfo.CertFile == "" || config.TlsInfo.KeyFile == "" {
			log.Error("tls config error, no cert or key file.")
			return errors.New("tls config error, no cert or key file.")
		}
		if config.TlsHost == "" {
			config.TlsHost = "0.0.0.0"
		}
	}
	return nil
}

func NewTLSConfig(tlsInfo TLSInfo) (*tls.Config, error) {

	cert, err := tls.LoadX509KeyPair(tlsInfo.CertFile, tlsInfo.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("error parsing X509 certificate/key pair: %v", err)
	}
	cert.Leaf, err = x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("error parsing certificate: %v", err)
	}

	// Create TLSConfig
	// We will determine the cipher suites that we prefer.
	config := tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	// Require client certificates as needed
	if tlsInfo.Verify {
		config.ClientAuth = tls.RequireAndVerifyClientCert
	}
	// Add in CAs if applicable.
	if tlsInfo.CaFile != "" {
		rootPEM, err := ioutil.ReadFile(tlsInfo.CaFile)
		if err != nil || rootPEM == nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		ok := pool.AppendCertsFromPEM([]byte(rootPEM))
		if !ok {
			return nil, fmt.Errorf("failed to parse root ca certificate")
		}
		config.ClientCAs = pool
	}

	return &config, nil
}
