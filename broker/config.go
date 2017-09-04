package broker

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"

	log "github.com/cihub/seelog"
)

const (
	CONFIGFILE = "hmq.config"
)

type Config struct {
	Worker  int       `json:"workerNum"`
	Host    string    `json:"host"`
	Port    string    `json:"port"`
	Cluster RouteInfo `json:"cluster"`
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
	Host   string   `json:"host"`
	Port   string   `json:"port"`
	Routes []string `json:"routes"`
}

type TLSInfo struct {
	Verify   bool   `json:"verify"`
	CaFile   string `json:"caFile"`
	CertFile string `json:"certFile"`
	KeyFile  string `json:"keyFile"`
}

func LoadConfig() (*Config, error) {

	content, err := ioutil.ReadFile(CONFIGFILE)
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

	if config.TlsPort != "" {
		if config.TlsInfo.CertFile == "" || config.TlsInfo.KeyFile == "" {
			log.Error("tls config error, no cert or key file.")
			return nil, err
		}
		if config.TlsHost == "" {
			config.TlsHost = "0.0.0.0"
		}
	}

	return &config, nil
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
