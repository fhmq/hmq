package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	mqtt5 "github.com/eclipse/paho.golang/packets"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

var ports = map[string]int{"single": 11883, "a": 21883, "b": 31883, "c": 41883}
var monitors = map[string]int{"single": 18080, "a": 28080, "b": 38080, "c": 48080}

func enabled(t *testing.T) {
	t.Helper()
	if os.Getenv("HMQ_COMPOSE_TEST") != "1" {
		t.Skip("set HMQ_COMPOSE_TEST=1 after starting Compose")
	}
}
func compose(t *testing.T, args ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	bin := os.Getenv("COMPOSE_BIN")
	prefix := []string{}
	if bin == "" {
		if p, err := exec.LookPath("docker-compose"); err == nil {
			bin = p
		} else {
			bin = "docker"
			prefix = []string{"compose"}
		}
	}
	argv := append(prefix, "-f", "../compose.yaml", "--profile", "single", "--profile", "cluster")
	argv = append(argv, args...)
	out, err := exec.CommandContext(ctx, bin, argv...).CombinedOutput()
	if err != nil {
		t.Fatalf("compose %v: %v\n%s", args, err, out)
	}
}
func client(t *testing.T, node string) mqtt.Client {
	t.Helper()
	var last error
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); {
		opts := mqtt.NewClientOptions().AddBroker(fmt.Sprintf("tcp://127.0.0.1:%d", ports[node])).SetClientID(fmt.Sprintf("compose-%d", time.Now().UnixNano())).SetAutoReconnect(false).SetConnectTimeout(3 * time.Second)
		c := mqtt.NewClient(opts)
		token := c.Connect()
		if token.WaitTimeout(5*time.Second) && token.Error() == nil {
			t.Cleanup(func() { c.Disconnect(0) })
			return c
		}
		last = token.Error()
		c.Disconnect(0)
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("connect %s: %v", node, last)
	return nil
}
func waitToken(t *testing.T, token mqtt.Token) {
	t.Helper()
	if !token.WaitTimeout(10*time.Second) || token.Error() != nil {
		t.Fatalf("MQTT operation: %v", token.Error())
	}
}
func exchange(t *testing.T, publisher, subscriber string, qos byte, retained bool) {
	t.Helper()
	sub := client(t, subscriber)
	pub := client(t, publisher)
	prefix := fmt.Sprintf("compose/check/%d", time.Now().UnixNano())
	topic := prefix + "/value"
	got := make(chan string, 4)
	waitToken(t, sub.Subscribe(prefix+"/+", qos, func(_ mqtt.Client, m mqtt.Message) { got <- string(m.Payload()) }))
	waitToken(t, pub.Publish(topic, qos, retained, "verified"))
	select {
	case s := <-got:
		if s != "verified" {
			t.Fatalf("payload %q", s)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("delivery missing")
	}
	if retained {
		late := client(t, subscriber)
		ch := make(chan bool, 1)
		waitToken(t, late.Subscribe(topic, qos, func(_ mqtt.Client, m mqtt.Message) { ch <- m.Retained() && string(m.Payload()) == "verified" }))
		select {
		case ok := <-ch:
			if !ok {
				t.Fatal("retained properties missing")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("retained missing")
		}
	}
}
func TestStandalone(t *testing.T) {
	enabled(t)
	for _, qos := range []byte{0, 1, 2} {
		t.Run(fmt.Sprint(qos), func(t *testing.T) { exchange(t, "single", "single", qos, true) })
	}
}
func TestMQTT5Properties(t *testing.T) {
	enabled(t)
	mqtt5RoundTrip(t, "single", "single")
}

func mqtt5RoundTrip(t *testing.T, publisher, subscriber string) {
	sub := client(t, subscriber)
	topic := fmt.Sprintf("compose/v5/%d", time.Now().UnixNano())
	got := make(chan string, 1)
	waitToken(t, sub.Subscribe(topic, 1, func(_ mqtt.Client, m mqtt.Message) { got <- string(m.Payload()) }))
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", ports[publisher]), 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	connect := &mqtt5.Connect{ProtocolName: "MQTT", ProtocolVersion: 5, CleanStart: true, ClientID: fmt.Sprintf("v5-%d", time.Now().UnixNano()), Properties: &mqtt5.Properties{}}
	if _, err = connect.WriteTo(conn); err != nil {
		t.Fatal(err)
	}
	p, err := mqtt5.ReadPacket(conn)
	if err != nil {
		t.Fatal(err)
	}
	ack, ok := p.Content.(*mqtt5.Connack)
	if !ok || ack.ReasonCode != 0 {
		t.Fatalf("CONNACK %v", p)
	}
	if publisher != "single" && (ack.Properties.MaximumQOS == nil || *ack.Properties.MaximumQOS != 1 || ack.Properties.RetainAvailable == nil || *ack.Properties.RetainAvailable != 0 || ack.Properties.SharedSubAvailable == nil || *ack.Properties.SharedSubAvailable != 0) {
		t.Fatalf("quorum capability flags missing: %v", ack)
	}
	receiver, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", ports[subscriber]), 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	_ = receiver.SetDeadline(time.Now().Add(10 * time.Second))
	hello := *connect
	hello.ClientID += "-subscriber"
	if _, err = hello.WriteTo(receiver); err != nil {
		t.Fatal(err)
	}
	p, err = mqtt5.ReadPacket(receiver)
	if err != nil {
		t.Fatal(err)
	}
	if a, ok := p.Content.(*mqtt5.Connack); !ok || a.ReasonCode != 0 {
		t.Fatalf("receiver CONNACK %v", p)
	}
	if _, err = (&mqtt5.Subscribe{PacketID: 2, Properties: &mqtt5.Properties{}, Subscriptions: []mqtt5.SubOptions{{Topic: topic, QoS: 1}}}).WriteTo(receiver); err != nil {
		t.Fatal(err)
	}
	p, err = mqtt5.ReadPacket(receiver)
	if err != nil {
		t.Fatal(err)
	}
	if a, ok := p.Content.(*mqtt5.Suback); !ok || len(a.Reasons) != 1 || a.Reasons[0] > 1 {
		t.Fatalf("SUBACK %v", p)
	}
	if _, err = (&mqtt5.Publish{Topic: topic, QoS: 1, PacketID: 1, Payload: []byte("mqtt5"), Properties: &mqtt5.Properties{ContentType: "text/plain", User: []mqtt5.User{{Key: "source", Value: "compose"}}}}).WriteTo(conn); err != nil {
		t.Fatal(err)
	}
	seenAck, seenPublish := false, false
	for !seenAck || !seenPublish {
		reader := conn
		if seenAck {
			reader = receiver
		}
		p, err = mqtt5.ReadPacket(reader)
		if err != nil {
			t.Fatal(err)
		}
		switch a := p.Content.(type) {
		case *mqtt5.Puback:
			if a.PacketID != 1 || a.ReasonCode >= 128 {
				t.Fatalf("PUBACK %v", a)
			}
			seenAck = true
		case *mqtt5.Publish:
			if string(a.Payload) != "mqtt5" || a.Properties.ContentType != "text/plain" || len(a.Properties.User) != 1 || a.Properties.User[0].Value != "compose" {
				t.Fatalf("publication properties lost: %v", a)
			}
			if _, err = (&mqtt5.Puback{PacketID: a.PacketID, Properties: &mqtt5.Properties{}}).WriteTo(receiver); err != nil {
				t.Fatal(err)
			}
			seenPublish = true
		default:
			t.Fatalf("unexpected %T", a)
		}
	}
	select {
	case s := <-got:
		if s != "mqtt5" {
			t.Fatal(s)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("mixed-version delivery missing")
	}
}

func TestStandaloneWill(t *testing.T) {
	enabled(t)
	sub := client(t, "single")
	topic := fmt.Sprintf("compose/will/%d", time.Now().UnixNano())
	got := make(chan string, 1)
	waitToken(t, sub.Subscribe(topic, 1, func(_ mqtt.Client, m mqtt.Message) { got <- string(m.Payload()) }))
	conn, err := net.DialTimeout("tcp", "127.0.0.1:11883", 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	hello := &mqtt5.Connect{ProtocolName: "MQTT", ProtocolVersion: 5, CleanStart: true, ClientID: fmt.Sprintf("will-%d", time.Now().UnixNano()), WillFlag: true, WillQOS: 1, WillTopic: topic, WillMessage: []byte("gone"), Properties: &mqtt5.Properties{}, WillProperties: &mqtt5.Properties{}}
	if _, err = hello.WriteTo(conn); err != nil {
		t.Fatal(err)
	}
	p, err := mqtt5.ReadPacket(conn)
	if err != nil {
		t.Fatal(err)
	}
	if a, ok := p.Content.(*mqtt5.Connack); !ok || a.ReasonCode != 0 {
		t.Fatalf("CONNACK %v", p)
	}
	_ = conn.Close()
	select {
	case s := <-got:
		if s != "gone" {
			t.Fatal(s)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("will publication missing")
	}
}

type health struct {
	State          string `json:"state"`
	LeaderID       string `json:"leaderID"`
	Cursor         uint64 `json:"cursor"`
	ConsumerFailed bool   `json:"consumerFailed"`
}

func state(node string) (health, error) {
	c := http.Client{Timeout: time.Second}
	r, err := c.Get(fmt.Sprintf("http://127.0.0.1:%d/api/v1/cluster/quorum", monitors[node]))
	if err != nil {
		return health{}, err
	}
	defer r.Body.Close()
	var h health
	err = json.NewDecoder(r.Body).Decode(&h)
	return h, err
}
func leader(t *testing.T, nodes []string) string {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			h, err := state(n)
			if err == nil && strings.EqualFold(h.State, "leader") && !h.ConsumerFailed {
				ready := true
				for _, peer := range nodes {
					other, e := state(peer)
					if e != nil || other.LeaderID != n || other.ConsumerFailed {
						ready = false
						break
					}
				}
				if ready {
					return n
				}
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatal("no healthy leader elected")
	return ""
}
func TestQuorumLifecycle(t *testing.T) {
	enabled(t)
	nodes := []string{"a", "b", "c"}
	initial := leader(t, nodes)
	t.Logf("initial leader: %s", initial)
	followers := []string{}
	for _, n := range nodes {
		if n != initial {
			followers = append(followers, n)
		}
	}
	exchange(t, followers[0], followers[1], 1, false)
	mqtt5RoundTrip(t, followers[0], followers[1])
	t.Cleanup(func() { compose(t, "start", "a", "b", "c") })
	compose(t, "stop", "-t", "3", initial)
	next := leader(t, followers)
	t.Logf("leader after stopping %s: %s", initial, next)
	exchange(t, followers[0], followers[1], 1, false)
	other := followers[0]
	if other == next {
		other = followers[1]
	}
	compose(t, "stop", "-t", "3", other)
	lone := client(t, next)
	token := lone.Publish(fmt.Sprintf("compose/no-majority/%d", time.Now().UnixNano()), 1, false, "must-not-ack")
	if token.WaitTimeout(8*time.Second) && token.Error() == nil {
		t.Fatal("QoS1 accepted without majority")
	}
	t.Logf("minority publication received no success acknowledgment (client error: %v)", token.Error())
	compose(t, "start", "a", "b", "c")
	leader(t, nodes)
	exchange(t, "a", "c", 1, false)
	before := map[string]uint64{}
	for _, n := range nodes {
		h, err := state(n)
		if err != nil {
			t.Fatal(err)
		}
		before[n] = h.Cursor
	}
	t.Logf("cursors before full restart: %v", before)
	compose(t, "stop", "-t", "3", "a", "b", "c")
	compose(t, "start", "a", "b", "c")
	leader(t, nodes)
	deadline := time.Now().Add(30 * time.Second)
	for _, n := range nodes {
		for {
			h, err := state(n)
			if err == nil && h.Cursor >= before[n] {
				t.Logf("recovered %s cursor: %d", n, h.Cursor)
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("cursor not recovered on %s", n)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	exchange(t, "b", "a", 1, false)
}
