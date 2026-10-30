package broker

import (
	stdjson "encoding/json"
	"strings"
	"testing"
)

func TestOnlineOfflineInfoDoesNotExposePassword(t *testing.T) {
	payload, err := stdjson.Marshal(OnlineOfflineMsg{ClientInfo: Info{ClientID: "client", Username: "user"}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.ToLower(string(payload)), "password") {
		t.Fatalf("notification contains password field: %s", payload)
	}
}
