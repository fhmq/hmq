package acl

import (
	"bufio"
	"errors"
	"io"
	"os"
	"strings"
)

const (
	SUB      = "1"
	PUB      = "2"
	PUBSUB   = "3"
	CLIENTID = "clientid"
	USERNAME = "username"
	IP       = "ip"
	ALLOW    = "allow"
	DENY     = "deny"
)

type AuthInfo struct {
	Auth   string
	Typ    string
	Val    string
	PubSub string
	Topics []string
}

type ACLConfig struct {
	File string
	Info []*AuthInfo
}

func AclConfigLoad(file string) (*ACLConfig, error) {
	aclconifg := &ACLConfig{
		File: file,
		Info: make([]*AuthInfo, 0, 4),
	}
	err := aclconifg.Parse()
	if err != nil {
		return nil, err
	}
	return aclconifg, err
}

func (c *ACLConfig) Parse() error {
	f, err := os.Open(c.File)
	defer f.Close()
	if err != nil {
		return err
	}
	buf := bufio.NewReader(f)
	var parseErr error
	for {
		line, err := buf.ReadString('\n')
		line = strings.TrimSpace(line)
		if isCommentOut(line) {
			continue
		}
		if line == "" {
			return parseErr
		}
		// fmt.Println(line)
		tmpArr := strings.Fields(line)
		if len(tmpArr) != 5 {
			parseErr = errors.New("\"" + line + "\" format is error")
			break
		}
		if tmpArr[0] != ALLOW && tmpArr[0] != DENY {
			parseErr = errors.New("\"" + line + "\" format is error")
			break
		}
		if tmpArr[1] != CLIENTID && tmpArr[1] != USERNAME && tmpArr[1] != IP {
			parseErr = errors.New("\"" + line + "\" format is error")
			break
		}
		if tmpArr[3] != PUB && tmpArr[3] != SUB && tmpArr[3] != PUBSUB {
			parseErr = errors.New("\"" + line + "\" format is error")
			break
		}
		// var pubsub int
		// pubsub, err = strconv.Atoi(tmpArr[3])
		// if err != nil {
		// 	parseErr = errors.New("\"" + line + "\" format is error")
		// 	break
		// }
		topicStr := strings.Replace(tmpArr[4], " ", "", -1)
		topicStr = strings.Replace(topicStr, "\n", "", -1)
		topics := strings.Split(topicStr, ",")
		tmpAuth := &AuthInfo{
			Auth:   tmpArr[0],
			Typ:    tmpArr[1],
			Val:    tmpArr[2],
			Topics: topics,
			PubSub: tmpArr[3],
		}
		c.Info = append(c.Info, tmpAuth)
		if err != nil {
			if err != io.EOF {
				parseErr = err
			}
			break
		}
	}
	return parseErr
}
func isCommentOut(line string) bool {
	if strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") || strings.HasPrefix(line, "//") || strings.HasPrefix(line, "*") {
		return true
	} else {
		return false
	}
}
