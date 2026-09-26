package downloader

import (
	"encoding/binary"
	"net"
	"reflect"
	"sync"
	"testing"

	"sainttorrent/pkg/dht"
	"sainttorrent/pkg/peer"
)

// TestPeerLoopActsOnPortChangesOnly is the regression test for the PORT message
// scan: every BEP 5 PORT message called DHT.AddNode, which walks the whole routing
// table under the DHT lock, with no per-connection limit. Repeats of the port
// already fed to the DHT are now free, and only maxPeerDHTPortUpdates changes per
// connection are acted on.
func TestPeerLoopActsOnPortChangesOnly(t *testing.T) {
	var (
		mu    sync.Mutex
		ports []uint16
	)
	origAdd := addDHTNode
	addDHTNode = func(_ *dht.DHT, _ net.IP, port uint16) {
		mu.Lock()
		ports = append(ports, port)
		mu.Unlock()
	}
	defer func() { addDHTNode = origAdd }()

	sess := newWireTestSession(t, 4, 16)
	sessionDHT, err := dht.NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("start DHT: %v", err)
	}
	defer sessionDHT.Close()
	sess.mu.Lock()
	sess.DHT = sessionDHT
	sess.mu.Unlock()
	w := startWirePeer(t, sess, 6270, fastReserved())

	sendPort := func(port uint16) {
		payload := make([]byte, 2)
		binary.BigEndian.PutUint16(payload, port)
		w.send(&peer.Message{ID: peer.MsgPort, Payload: payload})
	}
	for i := 0; i < 50; i++ {
		sendPort(7000)
	}
	for _, port := range []uint16{7001, 7001, 7000, 7002, 7003, 7004} {
		sendPort(port)
	}
	w.barrier()

	mu.Lock()
	defer mu.Unlock()
	if want := []uint16{7000, 7001, 7000, 7002}; !reflect.DeepEqual(ports, want) {
		t.Fatalf("AddNode called for ports %v, want %v", ports, want)
	}
}
