package bridge

import (
	"fmt"
	"testing"
)

//Test_topicMatch is here to double check the topic matching logic
func Test_topicMatch(t *testing.T) {

	tests := []struct {
		name   string
		topic  string
		filter string
		want   bool
	}{
		// Some sample test cases
		{name: "Simple", topic: "test", filter: "test", want: true},
		{name: "Simple", topic: "test/cat", filter: "test/+", want: true},
		{name: "Simple", topic: "test/cat/breed", filter: "test/+", want: false},
		{name: "Simple", topic: "test/cat", filter: "test/#", want: true},
		{name: "Simple", topic: "test/cat/banana", filter: "test/#", want: true},
		{name: "Simple", topic: "test/cat/banana", filter: "test/+", want: false},
		{name: "Simple", topic: "test/dog/banana", filter: "test/cat/+", want: false},
		{name: "Simple", topic: "test/cat/banana", filter: "test/+/banana", want: true},
	}

	for _, tt := range tests {
		fmt.Println(tt)
		t.Run(tt.name, func(t *testing.T) {
			if got := topicMatch(tt.topic, tt.filter); got != tt.want {
				t.Errorf("topicMatch() = %v, want %v", got, tt.want)
			}
		})
	}
}
