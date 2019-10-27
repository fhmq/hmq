package acl

import (
	"bytes"
	"errors"
	"strings"
)

func SubscribeTopicSpilt(topic string) ([]string, error) {
	subject := []byte(topic)
	if bytes.IndexByte(subject, '#') != -1 {
		if bytes.IndexByte(subject, '#') != len(subject)-1 {
			return nil, errors.New("Topic format error with index of #")
		}
	}
	re := strings.Split(topic, "/")
	for i, v := range re {
		if i != 0 && i != (len(re)-1) {
			if v == "" {
				return nil, errors.New("Topic format error with index of //")
			}
			if strings.Contains(v, "+") && v != "+" {
				return nil, errors.New("Topic format error with index of +")
			}
		} else {
			if v == "" {
				re[i] = "/"
			}
		}
	}
	return re, nil

}

func PublishTopicSpilt(topic string) ([]string, error) {
	subject := []byte(topic)
	if bytes.IndexByte(subject, '#') != -1 || bytes.IndexByte(subject, '+') != -1 {
		return nil, errors.New("Publish Topic format error with + and #")
	}
	re := strings.Split(topic, "/")
	for i, v := range re {
		if v == "" {
			if i != 0 && i != (len(re)-1) {
				return nil, errors.New("Topic format error with index of //")
			} else {
				re[i] = "/"
			}
		}

	}
	return re, nil
}
