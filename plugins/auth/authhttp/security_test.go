package authhttp

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func TestAuthorizationIsNotShared(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/auth" && r.FormValue("clientid") == "allowed" {
			return
		}
		if r.URL.Path == "/acl" && r.URL.Query().Get("username") == "allowed" {
			return
		}
		w.WriteHeader(http.StatusForbidden)
	}))
	defer s.Close()
	a := &authHTTP{client: s.Client(), config: Config{AuthURL: s.URL + "/auth", ACLURL: s.URL + "/acl"}}
	if !a.CheckConnect("allowed", "user", "pass") || a.CheckConnect("denied", "user", "pass") {
		t.Fatal("connect authorization leaked between clients")
	}
	if !a.CheckACL("1", "c", "allowed", "", "topic") || a.CheckACL("1", "c", "denied", "", "topic") {
		t.Fatal("ACL authorization leaked between users")
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); a.CheckConnect("denied", "user", "pass") }()
	}
	wg.Wait()
}
