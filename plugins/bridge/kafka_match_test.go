package bridge

import "testing"

func TestMatchTopicFilters(t *testing.T) {
	tests := []struct {
		filter, topic string
		want          bool
	}{
		{"a/#", "a", true}, {"a/+", "a/", true}, {"a/+", "a", false},
		{"#", "$SYS/status", false}, {"$SYS/#", "$SYS/status", true},
		{"a/#/b", "a/b", false}, {"a+", "abc", false},
		{"", "a", false}, {"#", "", false}, {"#", "a/+", false},
	}
	for _, tt := range tests {
		if got := matchTopic(tt.filter, tt.topic); got != tt.want {
			t.Errorf("matchTopic(%q, %q) = %v, want %v", tt.filter, tt.topic, got, tt.want)
		}
	}
}
