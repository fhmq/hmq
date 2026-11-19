package broker

import "testing"

func TestDisabledBridgeJSONDoesNotEnablePlugin(t *testing.T) {
	for _, name := range []string{"none", ""} {
		var c Config
		if err := json.Unmarshal([]byte(`{"plugins":{"auth":"mock","bridge":"`+name+`"}}`), &c); err != nil {
			t.Fatal(err)
		}
		if c.Plugin.Bridge != nil {
			t.Fatalf("disabled bridge %q creates a plugin", name)
		}
		if c.Plugin.Auth == nil {
			t.Fatal("mock authentication not configured")
		}
	}
}
