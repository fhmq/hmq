package authhttp

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/fhmq/hmq/logger"
	"go.uber.org/zap"
)

const (
	//AuthHTTP plugin name
	AuthHTTP = "authhttp"
)

var (
	config     Config
	log        = logger.Get().Named("http")
	httpClient *http.Client
)

//Config device kafka config
type Config struct {
	AuthURL  string `json:"auth"`
	ACLURL   string `json:"onSubscribe"`
	SuperURL string `json:"onPublish"`
}

//Init init kafak client
func Init() {
	content, err := ioutil.ReadFile("/plugins/authhttp/http.json")
	if err != nil {
		log.Fatal("Read config file error: ", zap.Error(err))
	}
	// log.Info(string(content))

	err = json.Unmarshal(content, &config)
	if err != nil {
		log.Fatal("Unmarshal config file error: ", zap.Error(err))
	}

	httpClient = &http.Client{
		Transport: &http.Transport{
			MaxConnsPerHost:     100,
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
		},
		Timeout: time.Second * 100,
	}

}

//CheckAuth check mqtt connect
func CheckAuth(clientID, username, password string) bool {
	data := url.Values{}
	data.Add("username", username)
	data.Add("clientid", clientID)
	data.Add("password", password)

	req, err := http.NewRequest("POST", config.AuthURL, strings.NewReader(data.Encode()))
	if err != nil {
		log.Error("new request super: ", zap.Error(err))
		return false
	}
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Content-Length", strconv.Itoa(len(data.Encode())))

	resp, err := httpClient.Do(req)
	if err != nil {
		log.Error("request super: ", zap.Error(err))
		return false
	}

	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return true
	}
	return false
}

//CheckSuper check mqtt connect
func CheckSuper(clientID, username, password string) bool {
	data := url.Values{}
	data.Add("username", username)
	data.Add("clientid", clientID)
	data.Add("password", password)

	req, err := http.NewRequest("POST", config.SuperURL, strings.NewReader(data.Encode()))
	if err != nil {
		log.Error("new request super: ", zap.Error(err))
		return false
	}
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Content-Length", strconv.Itoa(len(data.Encode())))

	resp, err := httpClient.Do(req)
	if err != nil {
		log.Error("request super: ", zap.Error(err))
		return false
	}

	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return true
	}
	return false
}

//CheckACL check mqtt connect
func CheckACL(username, access, topic string) bool {
	req, err := http.NewRequest("GET", config.ACLURL, nil)
	if err != nil {
		log.Error("get acl: ", zap.Error(err))
		return false
	}

	data := req.URL.Query()

	data.Add("username", username)
	data.Add("topic", topic)
	data.Add("access", access)
	req.URL.RawQuery = data.Encode()
	//	log.Debugf("req is :%v", req)
	resp, err := httpClient.Do(req)
	if err != nil {
		log.Error("request acl: ", zap.Error(err))
		return false
	}

	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return true
	}
	return false
}
