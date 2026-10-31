package broker

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/fhmq/hmq/v4/logger"
	"github.com/fhmq/hmq/v4/plugins/auth"
	"github.com/fhmq/hmq/v4/plugins/bridge"
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary
var clusterNodeIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

type Config struct {
	Worker          int       `json:"workerNum"`
	HTTPPort        string    `json:"httpPort"`
	Host            string    `json:"host"`
	Port            string    `json:"port"`
	Cluster         RouteInfo `json:"cluster"`
	Router          string    `json:"router"`
	TlsHost         string    `json:"tlsHost"`
	TlsPort         string    `json:"tlsPort"`
	WsPath          string    `json:"wsPath"`
	WsPort          string    `json:"wsPort"`
	WsTLS           bool      `json:"wsTLS"`
	TlsInfo         TLSInfo   `json:"tlsInfo"`
	Debug           string    `json:"debug"`
	Plugin          Plugins   `json:"plugins"`
	UnixFilePath    string    `json:"unixFilePath"`
	WindowsPipeName string    `json:"windowsPipeName"`
	RetainedLogPath string    `json:"retainedLogPath"`
	// ExperimentalSessionLogPath opens a standalone log; it does not enable session persistence.
	ExperimentalSessionLogPath string `json:"experimentalSessionLogPath"`
}

type Plugins struct {
	Auth   auth.Auth
	Bridge bridge.BridgeMQ
}

type NamedPlugins struct {
	Auth   string
	Bridge string
}

type RouteInfo struct {
	Host      string         `json:"host"`
	Port      string         `json:"port"`
	NodeID    string         `json:"nodeID"`
	ClusterID string         `json:"clusterID"`
	Secret    string         `json:"secret"`
	Listen    string         `json:"listen"`
	Seeds     []string       `json:"seeds"`
	TLS       ClusterTLSInfo `json:"tls"`
	Reliable  ReliableConfig `json:"reliable"`
	Quorum    QuorumConfig   `json:"quorum"`
}

// QuorumConfig enables the experimental fixed-membership single partition.
// Publications are restricted to leader QoS0/1, non-retained ordinary topics.
type QuorumConfig struct {
	Enabled bool              `json:"enabled"`
	DataDir string            `json:"dataDir"`
	Listen  string            `json:"listen"`
	Peers   map[string]string `json:"peers"`
}

// ReliableConfig describes the fixed membership and local durable logs.
type ReliableConfig struct {
	Enabled      bool     `json:"enabled"`
	MemberIDs    []string `json:"memberIDs"`
	EventLogPath string   `json:"eventLogPath"`
	InboxLogPath string   `json:"inboxLogPath"`
}

type ClusterTLSInfo struct {
	CertFile string `json:"certFile"`
	KeyFile  string `json:"keyFile"`
	CAFile   string `json:"caFile"`
}

type TLSInfo struct {
	Verify   bool   `json:"verify"`
	CaFile   string `json:"caFile"`
	CertFile string `json:"certFile"`
	KeyFile  string `json:"keyFile"`
}

var DefaultConfig *Config = &Config{
	Worker: 4096,
	Host:   "127.0.0.1",
	Port:   "1883",
}

var (
	log = logger.Prod().Named("broker")
)

func showHelp() {
	fmt.Printf("%s\n", usageStr)
	os.Exit(0)
}

func ConfigureConfig(args []string) (*Config, error) {
	config := &Config{}
	var (
		help       bool
		configFile string
	)
	fs := flag.NewFlagSet("hmq-broker", flag.ContinueOnError)
	fs.Usage = func() { fmt.Fprintln(fs.Output(), usageStr) }

	fs.BoolVar(&help, "h", false, "Show this message.")
	fs.BoolVar(&help, "help", false, "Show this message.")
	fs.IntVar(&config.Worker, "w", 1024, "worker num to process message, perfer (client num)/10.")
	fs.IntVar(&config.Worker, "worker", 1024, "worker num to process message, perfer (client num)/10.")
	fs.StringVar(&config.HTTPPort, "httpport", "", "Port to listen on.")
	fs.StringVar(&config.HTTPPort, "hp", "", "Port to listen on.")
	fs.StringVar(&config.Port, "port", "1883", "Port to listen on.")
	fs.StringVar(&config.Port, "p", "1883", "Port to listen on.")
	fs.StringVar(&config.UnixFilePath, "unixfilepath", "", "unix sock to listen on.")
	fs.StringVar(&config.Host, "host", "127.0.0.1", "Network host to listen on")
	fs.StringVar(&config.Cluster.Port, "cp", "", "Cluster port from which members can connect.")
	fs.StringVar(&config.Cluster.Port, "clusterport", "", "Cluster port from which members can connect.")
	fs.StringVar(&config.Router, "r", "", "Router who maintenance cluster info")
	fs.StringVar(&config.Router, "router", "", "Router who maintenance cluster info")
	fs.StringVar(&config.WsPort, "ws", "", "port for ws to listen on")
	fs.StringVar(&config.WsPort, "wsport", "", "port for ws to listen on")
	fs.StringVar(&config.WsPath, "wsp", "", "path for ws to listen on")
	fs.StringVar(&config.WsPath, "wspath", "", "path for ws to listen on")
	fs.StringVar(&configFile, "config", "", "config file for hmq")
	fs.StringVar(&configFile, "c", "", "config file for hmq")
	fs.StringVar(&config.Debug, "debug", "info", "enable Debug logging.")
	fs.StringVar(&config.Debug, "d", "info", "enable Debug logging.")

	fs.Bool("D", true, "enable Debug logging.")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	if help {
		showHelp()
		return nil, nil
	}

	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "D":
			config.Debug = "debug"
		}
	})
	var legacyFlag bool
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "r" || f.Name == "router" || f.Name == "cp" || f.Name == "clusterport" {
			legacyFlag = true
		}
	})
	if legacyFlag {
		return nil, errors.New("legacy router configuration is unsupported")
	}

	if configFile != "" {
		tmpConfig, e := LoadConfig(configFile)
		if e != nil {
			return nil, e
		} else {
			config = tmpConfig
		}
	}

	//Set the debug level of logs
	switch config.Debug {
	case "debug":
		log = logger.Debug().Named("broker")
	case "info":
		log = logger.Prod().Named("broker")
	case "release":
		log = logger.Release().Named("broker")
	default:
		log = logger.Debug().Named("broker")
	}

	if err := config.check(); err != nil {
		return nil, err
	}
	return config, nil

}

func LoadConfig(filename string) (*Config, error) {

	content, err := ioutil.ReadFile(filename)
	if err != nil {
		// log.Error("Read config file error: ", zap.Error(err))
		return nil, err
	}
	// log.Info(string(content))

	var fields map[string]jsoniter.RawMessage
	if err := json.Unmarshal(content, &fields); err != nil {
		return nil, errors.New("invalid configuration JSON")
	}
	for key, raw := range fields {
		if strings.EqualFold(key, "router") {
			return nil, errors.New("legacy router configuration is unsupported")
		}
		if !strings.EqualFold(key, "cluster") {
			continue
		}
		var clusterFields map[string]jsoniter.RawMessage
		if err := json.Unmarshal(raw, &clusterFields); err != nil {
			return nil, errors.New("invalid cluster configuration")
		}
		for name := range clusterFields {
			if strings.EqualFold(name, "host") || strings.EqualFold(name, "port") {
				return nil, errors.New("legacy cluster host/port configuration is unsupported")
			}
		}
	}
	var config Config
	err = json.Unmarshal(content, &config)
	if err != nil {
		// log.Error("Unmarshal config file error: ", zap.Error(err))
		return nil, errors.New("invalid configuration JSON or field type")
	}

	return &config, nil
}

func (p *Plugins) UnmarshalJSON(b []byte) error {
	var named NamedPlugins
	err := json.Unmarshal(b, &named)
	if err != nil {
		return err
	}
	p.Auth = auth.NewAuth(named.Auth)
	p.Bridge, err = bridge.NewBridgeMQ(named.Bridge)
	return err
}

func (config *Config) check() error {
	if err := validateSessionLogPath(config); err != nil {
		return err
	}
	if config.RetainedLogPath != "" && strings.TrimSpace(config.RetainedLogPath) == "" {
		return errors.New("retained log path must not be blank")
	}

	if config.Worker == 0 {
		config.Worker = 1024
	}
	if config.Worker < 0 {
		return errors.New("worker count must be positive")
	}

	if config.Port != "" {
		if config.Host == "" {
			config.Host = "0.0.0.0"
		}
	}

	if config.Router != "" || config.Cluster.Host != "" || config.Cluster.Port != "" {
		return errors.New("legacy router configuration is unsupported")
	}
	c := config.Cluster
	if err := validateReliableConfig(c, config.RetainedLogPath); err != nil {
		return err
	}
	if c.NodeID != "" || c.ClusterID != "" || c.Secret != "" || c.Listen != "" || len(c.Seeds) != 0 || c.TLS != (ClusterTLSInfo{}) {
		if strings.TrimSpace(c.NodeID) == "" || strings.TrimSpace(c.ClusterID) == "" || strings.TrimSpace(c.Secret) == "" || c.Listen == "" {
			return errors.New("cluster nodeID, clusterID, secret and listen are required")
		}
		if !clusterNodeIDPattern.MatchString(c.NodeID) {
			return errors.New("invalid cluster nodeID")
		}
		if !validClusterAddress(c.Listen, true) {
			return errors.New("invalid cluster listen address")
		}
		for _, seed := range c.Seeds {
			if !validClusterAddress(seed, false) {
				return errors.New("invalid cluster seed address")
			}
		}
		if strings.TrimSpace(c.TLS.CertFile) == "" || strings.TrimSpace(c.TLS.KeyFile) == "" || strings.TrimSpace(c.TLS.CAFile) == "" {
			return errors.New("cluster TLS certFile, keyFile and caFile are required")
		}
		if _, err := tls.LoadX509KeyPair(c.TLS.CertFile, c.TLS.KeyFile); err != nil {
			return errors.New("invalid cluster TLS certificate or private key")
		}
		ca, err := os.ReadFile(c.TLS.CAFile)
		if err != nil || !x509.NewCertPool().AppendCertsFromPEM(ca) {
			return errors.New("invalid cluster TLS CA file")
		}
	}

	if config.TlsPort != "" || (config.WsPort != "" && config.WsTLS) {
		if config.TlsInfo.CertFile == "" || config.TlsInfo.KeyFile == "" {
			log.Error("tls config error, no cert or key file.")
			return errors.New("tls config error, no cert or key file.")
		}
		if config.TlsPort != "" && config.TlsHost == "" {
			config.TlsHost = "0.0.0.0"
		}
	}
	return nil
}

func validateSessionLogPath(config *Config) error {
	if err := validateQuorumConfig(config); err != nil {
		return err
	}
	path := config.ExperimentalSessionLogPath
	if path == "" {
		return nil
	}
	if strings.TrimSpace(path) != path || strings.TrimSpace(path) == "" {
		return errors.New("experimental session log path must be nonblank and have no surrounding whitespace")
	}
	resolved, err := canonicalLogPath(path)
	if err != nil {
		return fmt.Errorf("invalid experimental session log path: %w", err)
	}
	for _, other := range []string{config.RetainedLogPath, config.Cluster.Reliable.EventLogPath, config.Cluster.Reliable.InboxLogPath} {
		if other == "" {
			continue
		}
		prior, err := canonicalLogPath(other)
		if err != nil {
			return fmt.Errorf("invalid log path: %w", err)
		}
		a, ea := os.Stat(path)
		b, eb := os.Stat(other)
		if resolved == prior || (ea == nil && eb == nil && os.SameFile(a, b)) {
			return errors.New("experimental session log path must be distinct from other log paths")
		}
	}
	return nil
}

func validateReliableConfig(c RouteInfo, retained string) error {
	r := c.Reliable
	if !r.Enabled {
		if len(r.MemberIDs) != 0 || r.EventLogPath != "" || r.InboxLogPath != "" {
			return errors.New("reliable settings require enabled mode")
		}
		return nil
	}
	if c.Listen == "" || c.NodeID == "" {
		return errors.New("reliable mode requires a configured cluster")
	}
	seen := make(map[string]bool)
	for _, id := range r.MemberIDs {
		if !clusterNodeIDPattern.MatchString(id) || seen[id] {
			return errors.New("invalid or duplicate reliable member ID")
		}
		seen[id] = true
	}
	if len(seen) < 2 || !seen[c.NodeID] {
		return errors.New("reliable members must include the local node and at least one peer")
	}
	paths := []string{r.EventLogPath, r.InboxLogPath}
	if retained != "" {
		paths = append(paths, retained)
	}
	resolved := make([]string, 0, len(paths))
	for _, path := range paths {
		if strings.TrimSpace(path) == "" || strings.TrimSpace(path) != path {
			return errors.New("reliable log paths must be nonblank")
		}
		p, err := canonicalLogPath(path)
		if err != nil {
			return fmt.Errorf("invalid log path: %w", err)
		}
		for i, prior := range resolved {
			if p == prior {
				return errors.New("reliable and retained log paths must be distinct")
			}
			a, ea := os.Stat(path)
			b, eb := os.Stat(paths[i])
			if ea == nil && eb == nil && os.SameFile(a, b) {
				return errors.New("reliable and retained log paths must be distinct")
			}
		}
		resolved = append(resolved, p)
	}
	return nil
}

func canonicalLogPath(path string) (string, error) {
	p, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	var tail []string
	for {
		resolved, err := filepath.EvalSymlinks(p)
		if err == nil {
			for i := len(tail) - 1; i >= 0; i-- {
				resolved = filepath.Join(resolved, tail[i])
			}
			return resolved, nil
		}
		if !os.IsNotExist(err) {
			return "", err
		}
		parent := filepath.Dir(p)
		if parent == p {
			return "", err
		}
		tail = append(tail, filepath.Base(p))
		p = parent
	}
}

func validClusterAddress(address string, listen bool) bool {
	host, port, err := net.SplitHostPort(address)
	if err != nil || (!listen && strings.TrimSpace(host) == "") || strings.TrimSpace(host) != host {
		return false
	}
	n, err := strconv.Atoi(port)
	return err == nil && n >= 1 && n <= 65535
}

func NewTLSConfig(tlsInfo TLSInfo) (*tls.Config, error) {

	cert, err := tls.LoadX509KeyPair(tlsInfo.CertFile, tlsInfo.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("error parsing X509 certificate/key pair: %w", err)
	}
	cert.Leaf, err = x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("error parsing certificate: %w", err)
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
		if err != nil {
			return nil, fmt.Errorf("read root ca certificate: %w", err)
		}
		pool := x509.NewCertPool()
		ok := pool.AppendCertsFromPEM(rootPEM)
		if !ok {
			return nil, fmt.Errorf("failed to parse root ca certificate")
		}
		config.ClientCAs = pool
	}

	return &config, nil
}
