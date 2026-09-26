// Package mse implements BitTorrent Message Stream Encryption/Protocol
// Encryption. MSE obfuscates the peer-wire handshake with Diffie-Hellman and,
// for the selected RC4 method, wraps the rest of the peer connection.
package mse

import (
	"bytes"
	"crypto/rand"
	"crypto/rc4"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net"
	"strings"
	"sync"
)

const (
	maxPadLen = 512
	keyLen    = 96
)

// CryptoMethod is an MSE post-handshake stream mode bit.
type CryptoMethod uint32

const (
	// CryptoMethodPlaintext means only the MSE header is obfuscated; the peer
	// wire stream after MSE runs in plaintext.
	CryptoMethodPlaintext CryptoMethod = 1
	// CryptoMethodRC4 means the peer wire stream after MSE is encrypted with RC4.
	CryptoMethodRC4 CryptoMethod = 2
)

// Policy controls whether peer connections use MSE.
type Policy int

const (
	// PolicyPrefer tries MSE first and allows plaintext peer-wire fallback.
	PolicyPrefer Policy = iota
	// PolicyRequire permits only MSE-encrypted peer connections.
	PolicyRequire
	// PolicyDisable uses plaintext peer-wire connections only.
	PolicyDisable
)

func (p Policy) String() string {
	switch p {
	case PolicyPrefer:
		return "prefer"
	case PolicyRequire:
		return "require"
	case PolicyDisable:
		return "disable"
	default:
		return fmt.Sprintf("unknown(%d)", int(p))
	}
}

// ParsePolicy parses prefer, require, or disable.
func ParsePolicy(s string) (Policy, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "prefer", "preferred":
		return PolicyPrefer, nil
	case "require", "required":
		return PolicyRequire, nil
	case "disable", "disabled", "off":
		return PolicyDisable, nil
	default:
		return PolicyPrefer, fmt.Errorf("invalid encryption policy %q (want prefer, require, or disable)", s)
	}
}

// SecretKeyIter visits acceptable torrent info hashes for a receiver. Returning
// false from callback stops iteration.
type SecretKeyIter func(callback func(skey []byte) bool)

// Result describes a completed MSE handshake.
type Result struct {
	Method    CryptoMethod
	SecretKey []byte
}

var (
	dhPrime       = mustPrime()
	dhGenerator   = big.NewInt(2)
	dhMinPeerKey  = big.NewInt(2)
	dhMaxPeerKey  = new(big.Int).Sub(dhPrime, big.NewInt(1))
	maxPadLenBig  = big.NewInt(maxPadLen + 1)
	req1          = []byte("req1")
	req2          = []byte("req2")
	req3          = []byte("req3")
	vc            [8]byte
	zeroPad       [maxPadLen]byte
	peerWirePlain = []byte{19, 'B', 'i', 't', 'T', 'o', 'r', 'r', 'e', 'n', 't', ' ', 'p', 'r', 'o', 't', 'o', 'c', 'o', 'l'}
)

// ErrNoSecretKeyMatch is returned by Receive when no configured secret key
// matches the initiator's torrent info hash.
var ErrNoSecretKeyMatch = errors.New("mse: no secret key matched")

// ErrNoCryptoMethod is returned when peers have no compatible MSE method.
var ErrNoCryptoMethod = errors.New("mse: no compatible crypto method")

func mustPrime() *big.Int {
	p := new(big.Int)
	if _, ok := p.SetString("FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD129024E088A67CC74020BBEA63B139B22514A08798E3404DDEF9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245E485B576625E7EC6F44C42E9A63A36210000000000090563", 16); !ok {
		panic("invalid MSE DH prime")
	}
	return p
}

// Conn is a net.Conn whose Read and Write paths may be transformed by the
// negotiated MSE stream method.
type Conn struct {
	net.Conn
	r io.Reader
	w io.Writer
}

func (c *Conn) Read(p []byte) (int, error) {
	return c.r.Read(p)
}

func (c *Conn) Write(p []byte) (int, error) {
	return c.w.Write(p)
}

func (c *Conn) UnderlyingConn() net.Conn {
	return c.Conn
}

type cipherReader struct {
	c *rc4.Cipher
	r io.Reader
}

func (r *cipherReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if n > 0 {
		r.c.XORKeyStream(p[:n], p[:n])
	}
	return n, err
}

type cipherWriter struct {
	c *rc4.Cipher
	w io.Writer

	// scratch is a grow-on-demand buffer reused across Write calls. Callers
	// (peer.Client) serialize writes under a single writeMu, so reusing this
	// buffer across calls is safe and keeps a permanently installed
	// cipherWriter allocation-free on the steady-state write path.
	scratch []byte
}

func (w *cipherWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if cap(w.scratch) < len(p) {
		w.scratch = make([]byte, len(p))
	}
	encrypted := w.scratch[:len(p)]
	w.c.XORKeyStream(encrypted, p)
	written := 0
	for written < len(encrypted) {
		n, err := w.w.Write(encrypted[written:])
		written += n
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, io.ErrNoProgress
		}
	}
	return len(p), nil
}

type asyncWriter struct {
	w    io.Writer
	ch   chan []byte
	done chan struct{}

	mu  sync.Mutex
	err error
}

func newAsyncWriter(w io.Writer) *asyncWriter {
	aw := &asyncWriter{
		w:    w,
		ch:   make(chan []byte, 8),
		done: make(chan struct{}),
	}
	go aw.run()
	return aw
}

func (w *asyncWriter) run() {
	defer close(w.done)
	for b := range w.ch {
		if _, err := writeFull(w.w, b); err != nil {
			w.mu.Lock()
			if w.err == nil {
				w.err = err
			}
			w.mu.Unlock()
			for range w.ch {
			}
			return
		}
	}
}

func (w *asyncWriter) post(b []byte) error {
	w.mu.Lock()
	err := w.err
	w.mu.Unlock()
	if err != nil {
		return err
	}
	cp := append([]byte(nil), b...)
	w.ch <- cp
	return nil
}

func (w *asyncWriter) close() error {
	close(w.ch)
	<-w.done
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

func writeFull(w io.Writer, p []byte) (int, error) {
	written := 0
	for written < len(p) {
		n, err := w.Write(p[written:])
		written += n
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, io.ErrNoProgress
		}
	}
	return written, nil
}

type handshaker struct {
	conn  net.Conn
	out   *asyncWriter
	s     [keyLen]byte
	skey  []byte
	skeys SecretKeyIter
}

// Initiate performs the outgoing MSE handshake using skey, normally the torrent
// info hash. initialPayload is delivered to the receiver before post-handshake
// stream bytes. methods is the offered CryptoMethod bitmask.
func Initiate(conn net.Conn, skey []byte, initialPayload []byte, methods CryptoMethod) (*Conn, Result, error) {
	if len(skey) == 0 {
		return nil, Result{}, errors.New("mse: empty secret key")
	}
	if methods == 0 {
		return nil, Result{}, ErrNoCryptoMethod
	}
	h := &handshaker{
		conn: conn,
		out:  newAsyncWriter(conn),
		skey: append([]byte(nil), skey...),
	}
	rw, res, err := h.doInitiator(initialPayload, methods)
	if closeErr := h.out.close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return nil, Result{}, err
	}
	return rw, res, nil
}

// Receive performs the incoming MSE handshake. selectMethod chooses one method
// from the initiator-provided bitmask and must return either CryptoMethodRC4,
// CryptoMethodPlaintext, or 0 to reject the peer.
func Receive(conn net.Conn, skeys SecretKeyIter, selectMethod func(CryptoMethod) CryptoMethod) (*Conn, Result, error) {
	if skeys == nil {
		return nil, Result{}, errors.New("mse: nil secret-key iterator")
	}
	if selectMethod == nil {
		selectMethod = SelectRC4
	}
	h := &handshaker{
		conn:  conn,
		out:   newAsyncWriter(conn),
		skeys: skeys,
	}
	rw, res, err := h.doReceiver(selectMethod)
	if closeErr := h.out.close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return nil, Result{}, err
	}
	return rw, res, nil
}

// SelectRC4 chooses RC4 if the peer offered it.
func SelectRC4(provided CryptoMethod) CryptoMethod {
	if provided&CryptoMethodRC4 != 0 {
		return CryptoMethodRC4
	}
	return 0
}

func (h *handshaker) doInitiator(initialPayload []byte, methods CryptoMethod) (*Conn, Result, error) {
	if len(initialPayload) > math.MaxUint16 {
		return nil, Result{}, errors.New("mse: initial payload too large")
	}
	x, ya, err := newKeyPair()
	if err != nil {
		return nil, Result{}, err
	}
	// Ya and PadA leave as one write: the pad exists to hide the fixed
	// 96-byte key length, which a lone 96-byte first segment gives away.
	if err := h.postPadded(ya); err != nil {
		return nil, Result{}, err
	}
	var yb [keyLen]byte
	if _, err := io.ReadFull(h.conn, yb[:]); err != nil {
		return nil, Result{}, fmt.Errorf("mse: read public key: %w", err)
	}
	if err := h.deriveSecret(x, yb[:]); err != nil {
		return nil, Result{}, err
	}

	// HASH('req1', S), HASH('req2', SKEY) xor HASH('req3', S) and
	// ENCRYPT(VC, crypto_provide, len(PadC), PadC, len(IA), IA) also leave
	// as one write, as libtorrent sends them; over uTP each write otherwise
	// costs its own round of acks.
	writeCipher, err := h.newCipher(true)
	if err != nil {
		return nil, Result{}, err
	}
	req2Hash := hash(req2, h.skey)
	xorInPlace(req2Hash, req2Hash, hash(req3, h.s[:]))
	frame := buildCryptoFrame(methods, initialPayload, true)
	flight := make([]byte, 0, 2*sha1.Size+len(frame))
	flight = append(flight, hash(req1, h.s[:])...)
	flight = append(flight, req2Hash...)
	flight = append(flight, frame...)
	writeCipher.XORKeyStream(flight[2*sha1.Size:], flight[2*sha1.Size:])
	if err := h.out.post(flight); err != nil {
		return nil, Result{}, err
	}

	readCipher, err := h.newCipher(false)
	if err != nil {
		return nil, Result{}, err
	}
	encryptedVC := make([]byte, len(vc))
	readCipher.XORKeyStream(encryptedVC, vc[:])
	if err := readUntil(io.LimitReader(h.conn, int64(maxPadLen+len(vc))), encryptedVC); err != nil {
		return nil, Result{}, fmt.Errorf("mse: receiver verification sync failed: %w", err)
	}

	cr := &cipherReader{c: readCipher, r: h.conn}
	method, err := readUint32(cr)
	if err != nil {
		return nil, Result{}, err
	}
	padLen, err := readUint16(cr)
	if err != nil {
		return nil, Result{}, err
	}
	if err := discardHandshakePad(cr, padLen); err != nil {
		return nil, Result{}, err
	}
	// crypto_select must name exactly one method, and one we offered.
	selected := CryptoMethod(method)
	if !singleMethod(selected) || selected&methods == 0 {
		return nil, Result{}, fmt.Errorf("%w: receiver selected %x from offered %x", ErrNoCryptoMethod, method, methods)
	}

	conn, err := h.wrapConn(selected, nil, readCipher, writeCipher)
	if err != nil {
		return nil, Result{}, err
	}
	return conn, Result{Method: selected, SecretKey: append([]byte(nil), h.skey...)}, nil
}

func (h *handshaker) doReceiver(selectMethod func(CryptoMethod) CryptoMethod) (*Conn, Result, error) {
	// Nothing is sent until the initiator's key has arrived, as the spec and
	// libtorrent order it. Answering first would let one spoofed uTP packet
	// make us send, and retransmit, Yb to its forged source.
	var ya [keyLen]byte
	if _, err := io.ReadFull(h.conn, ya[:]); err != nil {
		return nil, Result{}, fmt.Errorf("mse: read public key: %w", err)
	}
	x, yb, err := newKeyPair()
	if err != nil {
		return nil, Result{}, err
	}
	if err := h.deriveSecret(x, ya[:]); err != nil {
		return nil, Result{}, err
	}
	// Yb and PadB leave as one write, for the same reason as Ya and PadA.
	if err := h.postPadded(yb); err != nil {
		return nil, Result{}, err
	}
	if err := readUntil(io.LimitReader(h.conn, maxPadLen+sha1.Size), hash(req1, h.s[:])); err != nil {
		return nil, Result{}, fmt.Errorf("mse: initiator verification sync failed: %w", err)
	}

	var keyHashXor [sha1.Size]byte
	if _, err := io.ReadFull(h.conn, keyHashXor[:]); err != nil {
		return nil, Result{}, err
	}
	if err := h.matchSecretKey(keyHashXor[:]); err != nil {
		return nil, Result{}, err
	}

	readCipher, err := h.newCipher(true)
	if err != nil {
		return nil, Result{}, err
	}
	cr := &cipherReader{c: readCipher, r: h.conn}
	var gotVC [len(vc)]byte
	if _, err := io.ReadFull(cr, gotVC[:]); err != nil {
		return nil, Result{}, err
	}
	if gotVC != vc {
		return nil, Result{}, errors.New("mse: invalid initiator verification constant")
	}
	provided, err := readUint32(cr)
	if err != nil {
		return nil, Result{}, err
	}
	padLen, err := readUint16(cr)
	if err != nil {
		return nil, Result{}, err
	}
	if err := discardHandshakePad(cr, padLen); err != nil {
		return nil, Result{}, err
	}
	iaLen, err := readUint16(cr)
	if err != nil {
		return nil, Result{}, err
	}
	initialPayload := make([]byte, iaLen)
	if iaLen > 0 {
		if _, err := io.ReadFull(cr, initialPayload); err != nil {
			return nil, Result{}, err
		}
	}

	selected := selectMethod(CryptoMethod(provided))
	if !singleMethod(selected) || CryptoMethod(provided)&selected == 0 {
		return nil, Result{}, fmt.Errorf("%w: initiator offered %x", ErrNoCryptoMethod, provided)
	}

	writeCipher, err := h.newCipher(false)
	if err != nil {
		return nil, Result{}, err
	}
	frame := buildCryptoFrame(selected, nil, false)
	writeCipher.XORKeyStream(frame, frame)
	if err := h.out.post(frame); err != nil {
		return nil, Result{}, err
	}

	conn, err := h.wrapConn(selected, initialPayload, readCipher, writeCipher)
	if err != nil {
		return nil, Result{}, err
	}
	return conn, Result{Method: selected, SecretKey: append([]byte(nil), h.skey...)}, nil
}

// singleMethod reports whether m is exactly one method we can run. The spec
// requires crypto_select to name one, and anything else has no stream mode.
func singleMethod(m CryptoMethod) bool {
	return m == CryptoMethodPlaintext || m == CryptoMethodRC4
}

// newKeyPair returns a fresh DH private key and its public key, left-padded to
// keyLen bytes.
func newKeyPair() (*big.Int, []byte, error) {
	x, err := randomPrivate()
	if err != nil {
		return nil, nil, err
	}
	y := new(big.Int).Exp(dhGenerator, x, dhPrime)
	return x, leftPad(y.Bytes(), keyLen), nil
}

// deriveSecret checks the peer's public key and sets S from it and our
// private key x.
func (h *handshaker) deriveSecret(x *big.Int, peerKey []byte) error {
	peerY := new(big.Int).SetBytes(peerKey)
	if !validPeerPublicKey(peerY) {
		return errors.New("mse: invalid peer public key")
	}
	secret := new(big.Int).Exp(peerY, x, dhPrime)
	copy(h.s[keyLen-len(secret.Bytes()):], secret.Bytes())
	return nil
}

func validPeerPublicKey(peerY *big.Int) bool {
	return peerY.Cmp(dhMinPeerKey) >= 0 && peerY.Cmp(dhMaxPeerKey) < 0
}

// postPadded posts key followed by 0-512 random pad bytes as one write.
func (h *handshaker) postPadded(key []byte) error {
	n, err := randomPadLen()
	if err != nil {
		return err
	}
	b := make([]byte, len(key)+n)
	copy(b, key)
	if _, err := io.ReadFull(rand.Reader, b[len(key):]); err != nil {
		return err
	}
	return h.out.post(b)
}

func (h *handshaker) matchSecretKey(got []byte) error {
	expectedReq3 := hash(req3, h.s[:])
	var match []byte
	h.skeys(func(skey []byte) bool {
		req2Hash := hash(req2, skey)
		xorInPlace(req2Hash, req2Hash, expectedReq3)
		if bytes.Equal(req2Hash, got) {
			match = append([]byte(nil), skey...)
			return false
		}
		return true
	})
	if match == nil {
		return ErrNoSecretKeyMatch
	}
	h.skey = match
	return nil
}

func (h *handshaker) newCipher(initiatorToReceiver bool) (*rc4.Cipher, error) {
	keyName := []byte("keyB")
	if initiatorToReceiver {
		keyName = []byte("keyA")
	}
	c, err := rc4.NewCipher(hash(keyName, h.s[:], h.skey))
	if err != nil {
		return nil, err
	}
	var burn [1024]byte
	c.XORKeyStream(burn[:], burn[:])
	return c, nil
}

// wrapConn returns an error rather than a nil *Conn for anything but a single
// known method: a nil *Conn stored in a net.Conn is a non-nil interface that
// panics on first use.
func (h *handshaker) wrapConn(method CryptoMethod, initialPayload []byte, readCipher, writeCipher *rc4.Cipher) (*Conn, error) {
	switch method {
	case CryptoMethodRC4:
		r := io.Reader(&cipherReader{c: readCipher, r: h.conn})
		if len(initialPayload) > 0 {
			r = io.MultiReader(bytes.NewReader(initialPayload), r)
		}
		return &Conn{Conn: h.conn, r: r, w: &cipherWriter{c: writeCipher, w: h.conn}}, nil
	case CryptoMethodPlaintext:
		r := io.Reader(h.conn)
		if len(initialPayload) > 0 {
			r = io.MultiReader(bytes.NewReader(initialPayload), r)
		}
		return &Conn{Conn: h.conn, r: r, w: h.conn}, nil
	default:
		return nil, fmt.Errorf("%w: cannot run method %x", ErrNoCryptoMethod, method)
	}
}

func buildCryptoFrame(method CryptoMethod, initialPayload []byte, includeInitialPayload bool) []byte {
	padLen, err := randomPadLen()
	if err != nil {
		panic(err)
	}
	var buf bytes.Buffer
	buf.Write(vc[:])
	writeUint32(&buf, uint32(method))
	writeUint16(&buf, uint16(padLen))
	buf.Write(zeroPad[:padLen])
	if includeInitialPayload {
		writeUint16(&buf, uint16(len(initialPayload)))
		buf.Write(initialPayload)
	}
	return buf.Bytes()
}

func discardHandshakePad(r io.Reader, padLen uint16) error {
	if padLen > maxPadLen {
		return fmt.Errorf("mse: pad length %d exceeds maximum %d", padLen, maxPadLen)
	}
	_, err := io.CopyN(io.Discard, r, int64(padLen))
	return err
}

func randomPrivate() (*big.Int, error) {
	var b [20]byte
	if _, err := io.ReadFull(rand.Reader, b[:]); err != nil {
		return nil, err
	}
	x := new(big.Int).SetBytes(b[:])
	if x.Sign() == 0 {
		x.SetInt64(1)
	}
	return x, nil
}

func randomPadLen() (int, error) {
	n, err := rand.Int(rand.Reader, maxPadLenBig)
	if err != nil {
		return 0, err
	}
	return int(n.Int64()), nil
}

func hash(parts ...[]byte) []byte {
	h := sha1.New()
	for _, p := range parts {
		_, _ = h.Write(p)
	}
	return h.Sum(nil)
}

func leftPad(b []byte, n int) []byte {
	if len(b) >= n {
		return b[len(b)-n:]
	}
	ret := make([]byte, n)
	copy(ret[n-len(b):], b)
	return ret
}

func xorInPlace(dst, a, b []byte) {
	for i := range dst {
		dst[i] = a[i] ^ b[i]
	}
}

func readUntil(r io.Reader, target []byte) error {
	if len(target) == 0 {
		return nil
	}
	window := make([]byte, len(target))
	matched := 0
	for {
		if _, err := io.ReadFull(r, window[matched:]); err != nil {
			return err
		}
		matched = suffixMatchLen(window, target)
		if matched == len(target) {
			return nil
		}
		copy(window, window[len(window)-matched:])
	}
}

func suffixMatchLen(a, b []byte) int {
	if len(b) > len(a) {
		b = b[:len(a)]
	}
	for i := len(b); i > 0; i-- {
		match := true
		for j := 0; j < i; j++ {
			if b[i-1-j] != a[len(a)-1-j] {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return 0
}

func readUint16(r io.Reader) (uint16, error) {
	var b [2]byte
	_, err := io.ReadFull(r, b[:])
	return binary.BigEndian.Uint16(b[:]), err
}

func readUint32(r io.Reader) (uint32, error) {
	var b [4]byte
	_, err := io.ReadFull(r, b[:])
	return binary.BigEndian.Uint32(b[:]), err
}

func writeUint16(w io.Writer, v uint16) {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], v)
	_, _ = w.Write(b[:])
}

func writeUint32(w io.Writer, v uint32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	_, _ = w.Write(b[:])
}

// LooksLikePlaintextHandshake reports whether b starts with the BitTorrent peer
// wire handshake prefix. It is exported for listener negotiation tests and for
// downloader routing without importing the peer package into mse.
func LooksLikePlaintextHandshake(b []byte) bool {
	return bytes.HasPrefix(b, peerWirePlain)
}

// PlaintextHandshakePrefixLen is the number of bytes needed to classify a
// plaintext BitTorrent peer-wire handshake.
func PlaintextHandshakePrefixLen() int {
	return len(peerWirePlain)
}
