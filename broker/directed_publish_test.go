package broker

import (
	"net"
	"testing"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

func TestDirectedPublishKeepsClientRegistered(t *testing.T) {
	server, peer := net.Pipe()
	defer server.Close()
	defer peer.Close()
	b := &Broker{}
	c := &client{conn: server, status: Connected}
	b.clients.Store("client", c)
	read := make(chan error, 1)
	go func() { _, err := packets.ReadPacket(peer); read <- err }()
	packet := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	packet.TopicName = "test"
	if err := b.PublishMessageByClientId(packet, "client"); err != nil {
		t.Fatal(err)
	}
	if err := <-read; err != nil {
		t.Fatal(err)
	}
	if got, ok := b.clients.Load("client"); !ok || got != c {
		t.Fatal("directed publish removed client")
	}
}
