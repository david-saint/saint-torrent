package mse

import (
	"bytes"
	"errors"
	"io"
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRC4HandshakeRoundTrip(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	_ = clientConn.SetDeadline(time.Now().Add(2 * time.Second))
	_ = serverConn.SetDeadline(time.Now().Add(2 * time.Second))

	skey := bytes.Repeat([]byte{0x42}, 20)
	type sideResult struct {
		conn *Conn
		res  Result
		err  error
	}
	initiatorDone := make(chan sideResult, 1)
	receiverDone := make(chan sideResult, 1)

	go func() {
		conn, res, err := Initiate(clientConn, skey, nil, CryptoMethodRC4)
		initiatorDone <- sideResult{conn: conn, res: res, err: err}
	}()
	go func() {
		conn, res, err := Receive(serverConn, singleSecret(skey), SelectRC4)
		receiverDone <- sideResult{conn: conn, res: res, err: err}
	}()

	initiator := <-initiatorDone
	receiver := <-receiverDone
	if initiator.err != nil {
		t.Fatalf("Initiate failed: %v", initiator.err)
	}
	if receiver.err != nil {
		t.Fatalf("Receive failed: %v", receiver.err)
	}
	if initiator.res.Method != CryptoMethodRC4 || receiver.res.Method != CryptoMethodRC4 {
		t.Fatalf("negotiated methods = initiator %d receiver %d, want RC4", initiator.res.Method, receiver.res.Method)
	}
	if !bytes.Equal(receiver.res.SecretKey, skey) {
		t.Fatalf("receiver selected secret %x, want %x", receiver.res.SecretKey, skey)
	}

	writeErr := make(chan error, 2)
	go func() {
		_, err := initiator.conn.Write([]byte("hello receiver"))
		writeErr <- err
	}()
	buf := make([]byte, len("hello receiver"))
	if _, err := io.ReadFull(receiver.conn, buf); err != nil {
		t.Fatalf("receiver read failed: %v", err)
	}
	if string(buf) != "hello receiver" {
		t.Fatalf("receiver read %q", buf)
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("initiator write failed: %v", err)
	}

	go func() {
		_, err := receiver.conn.Write([]byte("hello initiator"))
		writeErr <- err
	}()
	buf = make([]byte, len("hello initiator"))
	if _, err := io.ReadFull(initiator.conn, buf); err != nil {
		t.Fatalf("initiator read failed: %v", err)
	}
	if string(buf) != "hello initiator" {
		t.Fatalf("initiator read %q", buf)
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("receiver write failed: %v", err)
	}
}

func TestInitialPayloadDeliveredBeforeStream(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	_ = clientConn.SetDeadline(time.Now().Add(2 * time.Second))
	_ = serverConn.SetDeadline(time.Now().Add(2 * time.Second))

	skey := bytes.Repeat([]byte{0x12}, 20)
	initial := []byte("initial-payload")
	type sideResult struct {
		conn *Conn
		err  error
	}
	initiatorDone := make(chan sideResult, 1)
	receiverDone := make(chan sideResult, 1)

	go func() {
		conn, _, err := Initiate(clientConn, skey, initial, CryptoMethodRC4)
		initiatorDone <- sideResult{conn: conn, err: err}
	}()
	go func() {
		conn, _, err := Receive(serverConn, singleSecret(skey), SelectRC4)
		receiverDone <- sideResult{conn: conn, err: err}
	}()

	initiator := <-initiatorDone
	receiver := <-receiverDone
	if initiator.err != nil {
		t.Fatalf("Initiate failed: %v", initiator.err)
	}
	if receiver.err != nil {
		t.Fatalf("Receive failed: %v", receiver.err)
	}

	go func() {
		_, _ = initiator.conn.Write([]byte("-stream"))
	}()
	buf := make([]byte, len("initial-payload-stream"))
	if _, err := io.ReadFull(receiver.conn, buf); err != nil {
		t.Fatalf("receiver read failed: %v", err)
	}
	if string(buf) != "initial-payload-stream" {
		t.Fatalf("receiver read %q", buf)
	}
}

func TestReceiveRejectsUnknownSecretKey(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	_ = clientConn.SetDeadline(time.Now().Add(2 * time.Second))
	_ = serverConn.SetDeadline(time.Now().Add(2 * time.Second))

	initiatorSecret := bytes.Repeat([]byte{0x01}, 20)
	receiverSecret := bytes.Repeat([]byte{0x02}, 20)
	errCh := make(chan error, 2)
	go func() {
		_, _, err := Initiate(clientConn, initiatorSecret, nil, CryptoMethodRC4)
		errCh <- err
	}()
	go func() {
		_, _, err := Receive(serverConn, singleSecret(receiverSecret), SelectRC4)
		_ = serverConn.Close()
		errCh <- err
	}()

	var sawNoMatch bool
	for i := 0; i < 2; i++ {
		err := <-errCh
		if errors.Is(err, ErrNoSecretKeyMatch) {
			sawNoMatch = true
		}
	}
	if !sawNoMatch {
		t.Fatal("receiver did not reject the unknown secret key")
	}
}

func TestSelectRC4RejectsPlaintextOnlyOffer(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	_ = clientConn.SetDeadline(time.Now().Add(2 * time.Second))
	_ = serverConn.SetDeadline(time.Now().Add(2 * time.Second))

	skey := bytes.Repeat([]byte{0x33}, 20)
	errCh := make(chan error, 2)
	go func() {
		_, _, err := Initiate(clientConn, skey, nil, CryptoMethodPlaintext)
		errCh <- err
	}()
	go func() {
		_, _, err := Receive(serverConn, singleSecret(skey), SelectRC4)
		_ = serverConn.Close()
		errCh <- err
	}()

	var sawNoMethod bool
	for i := 0; i < 2; i++ {
		err := <-errCh
		if errors.Is(err, ErrNoCryptoMethod) {
			sawNoMethod = true
		}
	}
	if !sawNoMethod {
		t.Fatal("receiver did not reject a plaintext-only MSE offer")
	}
}

func TestPeerPublicKeyRangeRejectsDegenerateKeys(t *testing.T) {
	tests := []struct {
		name string
		key  *big.Int
	}{
		{"zero", big.NewInt(0)},
		{"one", big.NewInt(1)},
		{"prime-minus-one", new(big.Int).Sub(dhPrime, big.NewInt(1))},
		{"prime", new(big.Int).Set(dhPrime)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if validPeerPublicKey(tt.key) {
				t.Fatalf("degenerate key %s accepted", tt.key)
			}
		})
	}
}

func TestPeerPublicKeyRangeAcceptsValidBoundaries(t *testing.T) {
	tests := []struct {
		name string
		key  *big.Int
	}{
		{"two", big.NewInt(2)},
		{"prime-minus-two", new(big.Int).Sub(dhPrime, big.NewInt(2))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !validPeerPublicKey(tt.key) {
				t.Fatalf("valid key %s rejected", tt.key)
			}
		})
	}
}

func TestDiscardHandshakePadRejectsOversizedPad(t *testing.T) {
	pad := bytes.Repeat([]byte{0}, maxPadLen+1)
	r := bytes.NewReader(pad)
	err := discardHandshakePad(r, maxPadLen+1)
	if err == nil {
		t.Fatal("expected oversized pad to be rejected")
	}
	if !strings.Contains(err.Error(), "pad length") {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.Len() != len(pad) {
		t.Fatalf("oversized pad consumed %d bytes before rejection", len(pad)-r.Len())
	}
}

func TestDiscardHandshakePadConsumesBoundedPad(t *testing.T) {
	r := bytes.NewReader([]byte{1, 2, 3, 4})
	if err := discardHandshakePad(r, 3); err != nil {
		t.Fatalf("bounded pad rejected: %v", err)
	}
	if r.Len() != 1 {
		t.Fatalf("bounded pad consumed wrong length, remaining = %d", r.Len())
	}
}

func TestParsePolicy(t *testing.T) {
	tests := []struct {
		raw  string
		want Policy
	}{
		{"", PolicyPrefer},
		{"prefer", PolicyPrefer},
		{"required", PolicyRequire},
		{"off", PolicyDisable},
	}
	for _, tt := range tests {
		got, err := ParsePolicy(tt.raw)
		if err != nil {
			t.Fatalf("ParsePolicy(%q) failed: %v", tt.raw, err)
		}
		if got != tt.want {
			t.Fatalf("ParsePolicy(%q) = %v, want %v", tt.raw, got, tt.want)
		}
	}
	if _, err := ParsePolicy("bogus"); err == nil {
		t.Fatal("expected invalid policy error")
	}
}

// recordingConn records the size of every Write, which is how the handshake
// is cut into segments on the wire (peer conns run with TCP_NODELAY).
type recordingConn struct {
	net.Conn
	mu     sync.Mutex
	writes []int
}

func (c *recordingConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	c.writes = append(c.writes, len(p))
	c.mu.Unlock()
	return c.Conn.Write(p)
}

func (c *recordingConn) writeSizes() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]int(nil), c.writes...)
}

// TestReceiverWaitsForInitiatorKey checks that the receiver sends nothing
// until the initiator's public key has arrived. Sending Yb first let a single
// spoofed uTP packet make us send (and retransmit) 96+ bytes to its forged
// source.
func TestReceiverWaitsForInitiatorKey(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	done := make(chan error, 1)
	go func() {
		_, _, err := Receive(serverConn, singleSecret(bytes.Repeat([]byte{0x44}, 20)), SelectRC4)
		done <- err
	}()
	_ = clientConn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	n, err := clientConn.Read(make([]byte, keyLen))
	var ne net.Error
	if n != 0 || !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("receiver sent %d bytes (err=%v) before the initiator sent anything", n, err)
	}
	_ = clientConn.Close()
	if err := <-done; err == nil {
		t.Fatal("Receive succeeded on a closed conn")
	}
}

// TestHandshakeFlightsAreCoalesced checks that each side's handshake leaves in
// two writes: its key with its pad, then everything after the key exchange.
// Separate writes put an exact 96-byte segment at the start of every stream,
// which undoes the length obfuscation the pads exist for, and over uTP each
// write waits for its own acks.
func TestHandshakeFlightsAreCoalesced(t *testing.T) {
	clientPipe, serverPipe := net.Pipe()
	defer clientPipe.Close()
	defer serverPipe.Close()
	_ = clientPipe.SetDeadline(time.Now().Add(2 * time.Second))
	_ = serverPipe.SetDeadline(time.Now().Add(2 * time.Second))
	client := &recordingConn{Conn: clientPipe}
	server := &recordingConn{Conn: serverPipe}

	skey := bytes.Repeat([]byte{0x55}, 20)
	errs := make(chan error, 2)
	go func() {
		_, _, err := Initiate(client, skey, []byte("initial"), CryptoMethodRC4)
		errs <- err
	}()
	go func() {
		_, _, err := Receive(server, singleSecret(skey), SelectRC4)
		errs <- err
	}()
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("handshake: %v", err)
		}
	}
	for name, c := range map[string]*recordingConn{"initiator": client, "receiver": server} {
		sizes := c.writeSizes()
		if len(sizes) != 2 {
			t.Fatalf("%s wrote the handshake in %d writes %v, want 2", name, len(sizes), sizes)
		}
		if sizes[0] < keyLen || sizes[0] > keyLen+maxPadLen {
			t.Fatalf("%s first write is %d bytes, want its key and pad (%d-%d)", name, sizes[0], keyLen, keyLen+maxPadLen)
		}
	}
}

func singleSecret(skey []byte) SecretKeyIter {
	return func(callback func([]byte) bool) {
		callback(skey)
	}
}
