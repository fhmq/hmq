package authhttp

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/fhmq/hmq/v3/logger"
	"go.uber.org/zap"
)

// Config device kafka config
type Config struct {
	AuthURL  string `json:"auth"`
	ACLURL   string `json:"acl"`
	SuperURL string `json:"super"`
}

type authHTTP struct {
	client *http.Client
	config Config
}

var (
	log = logger.Get().Named("authhttp")
)

// Init init kafak client
func Init() *authHTTP {
	content, err := ioutil.ReadFile("./plugins/auth/authhttp/http.json")
	if err != nil {
		log.Fatal("Read config file error: ", zap.Error(err))
	}
	// log.Info(string(content))

	var config Config
	err = json.Unmarshal(content, &config)
	if err != nil {
		log.Fatal("Unmarshal config file error: ", zap.Error(err))
	}
	// fmt.Println("http: config: ", config)

	client := &http.Client{
		Transport: &http.Transport{
			MaxConnsPerHost:     100,
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
		},
		Timeout: time.Second * 100,
	}
	return &authHTTP{client: client, config: config}
}

// CheckConnect check mqtt connect
func (a *authHTTP) CheckConnect(clientID, username, password string) bool {

	data := url.Values{}
	data.Add("username", username)
	data.Add("clientid", clientID)
	data.Add("password", password)

	req, err := http.NewRequest("POST", a.config.AuthURL, strings.NewReader(data.Encode()))
	if err != nil {
		log.Error("new request super: ", zap.Error(err))
		return false
	}
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Content-Length", strconv.Itoa(len(data.Encode())))

	resp, err := a.client.Do(req)
	if err != nil {
		log.Error("request super: ", zap.Error(err))
		return false
	}

	defer resp.Body.Close()
	io.Copy(ioutil.Discard, resp.Body)
	if resp.StatusCode == http.StatusOK {
		return true
	}

	return false
}

// //CheckSuper check mqtt connect
// func CheckSuper(clientID, username, password string) bool {
// 	action := "connect"
// 	{
// 		aCache := checkCache(action, clientID, username, password, "")
// 		if aCache != nil {
// 			if aCache.password == password && aCache.username == username && aCache.action == action {
// 				return true
// 			}
// 		}
// 	}

// 	data := url.Values{}
// 	data.Add("username", username)
// 	data.Add("clientid", clientID)
// 	data.Add("password", password)

// 	req, err := http.NewRequest("POST", config.SuperURL, strings.NewReader(data.Encode()))
// 	if err != nil {
// 		log.Error("new request super: ", zap.Error(err))
// 		return false
// 	}
// 	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
// 	req.Header.Add("Content-Length", strconv.Itoa(len(data.Encode())))

// 	resp, err := httpClient.Do(req)
// 	if err != nil {
// 		log.Error("request super: ", zap.Error(err))
// 		return false
// 	}

// 	defer resp.Body.Close()
// 	io.Copy(ioutil.Discard, resp.Body)

// 	if resp.StatusCode == http.StatusOK {
// 		return true
// 	}
// 	return false
// }

// CheckACL check mqtt connect
func (a *authHTTP) CheckACL(action, clientID, username, ip, topic string) bool {

	req, err := http.NewRequest("GET", a.config.ACLURL, nil)
	if err != nil {
		log.Error("get acl: ", zap.Error(err))
		return false
	}

	data := req.URL.Query()

	data.Add("username", username)
	data.Add("topic", topic)
	data.Add("access", action)
	req.URL.RawQuery = data.Encode()
	// fmt.Println("req:", req)
	resp, err := a.client.Do(req)
	if err != nil {
		log.Error("request acl: ", zap.Error(err))
		return false
	}

	defer resp.Body.Close()
	io.Copy(ioutil.Discard, resp.Body)

	if resp.StatusCode == http.StatusOK {
		return true
	}
	return false
}
