package mse

import (
	"bytes"
	"errors"
	"io"
	"net"
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

func singleSecret(skey []byte) SecretKeyIter {
	return func(callback func([]byte) bool) {
		callback(skey)
	}
}
