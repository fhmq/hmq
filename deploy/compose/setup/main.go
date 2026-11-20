package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"time"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}
func main() {
	if len(os.Args) != 2 {
		panic("usage: setup RUNTIME_DIRECTORY")
	}
	dir := os.Args[1]
	if _, err := os.Stat(filepath.Join(dir, "ready")); err == nil {
		fmt.Println("existing local credentials preserved")
		return
	}
	check(os.MkdirAll(dir, 0755))
	write := func(name string, data []byte) {
		path := filepath.Join(dir, name)
		check(os.WriteFile(path, data, 0600))
		check(os.Chown(path, 65532, 65532))
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	check(err)
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "HMQ local evaluation CA"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(365 * 24 * time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	check(err)
	write("ca.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	secret := make([]byte, 32)
	_, err = rand.Read(secret)
	check(err)
	for i, id := range []string{"single", "a", "b", "c"} {
		cfg := map[string]any{"host": "0.0.0.0", "port": "1883", "httpPort": "8080", "workerNum": 16, "plugins": map[string]string{"auth": "mock", "bridge": "none"}}
		if id == "single" {
			cfg["experimentalSessionLogPath"] = "/data/sessions.wal"
			cfg["retainedLogPath"] = "/data/retained.wal"
		}
		if id != "single" {
			k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
			check(err)
			uri, err := url.Parse("urn:hmq:node:" + id)
			check(err)
			leaf := &x509.Certificate{SerialNumber: big.NewInt(int64(i + 1)), Subject: pkix.Name{CommonName: id}, DNSNames: []string{id}, URIs: []*url.URL{uri}, NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}}
			cert, err := x509.CreateCertificate(rand.Reader, leaf, ca, &k.PublicKey, key)
			check(err)
			write(id+".pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert}))
			priv, err := x509.MarshalECPrivateKey(k)
			check(err)
			write(id+".key", pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: priv}))
			seeds := []string{}
			for _, peer := range []string{"a", "b", "c"} {
				if peer != id {
					seeds = append(seeds, peer+":1993")
				}
			}
			cfg["cluster"] = map[string]any{"nodeID": id, "clusterID": "compose-local", "secret": hex.EncodeToString(secret), "listen": id + ":1993", "seeds": seeds, "tls": map[string]string{"certFile": "/runtime/" + id + ".pem", "keyFile": "/runtime/" + id + ".key", "caFile": "/runtime/ca.pem"}, "quorum": map[string]any{"enabled": true, "sessionPersistence": true, "dataDir": "/data/quorum", "listen": id + ":2993", "peers": map[string]string{"a": "a:2993", "b": "b:2993", "c": "c:2993"}}}
		}
		data, err := json.MarshalIndent(cfg, "", "  ")
		check(err)
		write(id+".json", data)
	}
	write("ready", []byte("local evaluation only\n"))
	fmt.Println("generated isolated local certificates and configuration")
}
