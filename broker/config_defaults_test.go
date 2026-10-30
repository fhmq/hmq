package broker

import "testing"

func TestDefaultCLIListenerConfig(t *testing.T) {
	c, err := ConfigureConfig(nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.Host != "127.0.0.1" || c.Port != "1883" {
		t.Fatalf("unexpected default listener: %s:%s", c.Host, c.Port)
	}
	if c.HTTPPort != "" || c.TlsPort != "" || c.WsPort != "" || c.Cluster.Listen != "" {
		t.Fatalf("optional listener unexpectedly enabled: %+v", c)
	}
}
