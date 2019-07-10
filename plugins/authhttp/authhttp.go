package authhttp

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/fhmq/hmq/logger"
	"go.uber.org/zap"
)

const (
	//AuthHTTP plugin name
	AuthHTTP = "authhttp"
)

var (
	config Config
	log    = logger.Get().Named("http")
)

//Config device kafka config
type Config struct {
	AuthURL  string `json:"auth"`
	ACLURL   string `json:"onSubscribe"`
	SuperURL string `json:"onPublish"`
}

//Init init kafak client
func Init() {
	content, err := ioutil.ReadFile("../../plugins/kafka/conf.json")
	if err != nil {
		log.Fatal("Read config file error: ", zap.Error(err))
	}
	// log.Info(string(content))

	err = json.Unmarshal(content, &config)
	if err != nil {
		log.Fatal("Unmarshal config file error: ", zap.Error(err))
	}

}

//CheckAuth check mqtt connect
func CheckAuth(clientID, username, password string) bool {
	payload := fmt.Sprintf("username=%s&password=%s&clientid=%s", username, password, clientID)
	resp, err := http.Post(config.AuthURL,
		"application/x-www-form-urlencoded",
		strings.NewReader(payload))
	if err != nil {
		log.Error("request acl: ", zap.Error(err))
	}

	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return true
	}
	return false
}

//CheckSuper check mqtt connect
func CheckSuper(clientID, username, password string) bool {
	payload := fmt.Sprintf("username=%s&password=%s&clientid=%s", username, password, clientID)
	resp, err := http.Post(config.SuperURL,
		"application/x-www-form-urlencoded",
		strings.NewReader(payload))
	if err != nil {
		log.Error("request acl: ", zap.Error(err))
	}

	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return true
	}
	return false
}

//CheckACL check mqtt connect
func CheckACL(username, access, topic string) bool {
	url := fmt.Sprintf(config.ACLURL+"?username=%s&access=%s&topic=%s", username, access, topic)
	resp, err := http.Get(url)
	if err != nil {
		// handle error
	}

	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return true
	}
	return false
}
