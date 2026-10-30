package broker

import (
	"sync"
	"testing"
)

func TestClientConcurrentCloseAndState(t *testing.T) {
	c := &client{status: Connected}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				c.isDisconnected()
				c.Close()
			}
		}()
	}
	wg.Wait()
	if !c.isDisconnected() {
		t.Fatal("client should be disconnected")
	}
}
