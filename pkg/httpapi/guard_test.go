package httpapi

import (
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"sainttorrent/pkg/downloader"
)

// A DNS-rebinding page reaches the loopback listener under its own host name;
// the Host check must refuse it before any snapshot is taken.
func TestHandlerRejectsRebindingHost(t *testing.T) {
	mgr := downloader.NewTorrentManager()
	defer mgr.Close()
	handler := NewHandler(mgr)

	for _, host := range []string{"rebind.attacker.example:16666", "rebind.attacker.example", "localhost.attacker.example:16666"} {
		req := httptest.NewRequest(http.MethodGet, "/stats", nil)
		req.Host = host
		req.Header.Set("Sec-Fetch-Site", "same-origin")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusMisdirectedRequest {
			t.Fatalf("Host %q: status = %d, want %d", host, rec.Code, http.StatusMisdirectedRequest)
		}
		if strings.Contains(rec.Body.String(), "torrents") {
			t.Fatalf("Host %q: stats leaked in body %q", host, rec.Body.String())
		}
	}
}

func TestHandlerAcceptsLocalHosts(t *testing.T) {
	handler := NewHandler(nil, "stats.lan")
	for _, host := range []string{
		"127.0.0.1:16666", "127.0.0.1", "[::1]:16666", "[::1]", "localhost:16666",
		"LOCALHOST", "192.168.1.20:16666", "stats.lan:16666", "Stats.LAN", "",
	} {
		req := httptest.NewRequest(http.MethodGet, "/stats", nil)
		req.Host = host
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("Host %q: status = %d, want %d; body=%s", host, rec.Code, http.StatusOK, rec.Body.String())
		}
		if got := rec.Header().Get("X-Content-Type-Options"); got != "nosniff" {
			t.Fatalf("X-Content-Type-Options = %q, want nosniff", got)
		}
		if got := rec.Header().Get("Cache-Control"); got != "no-store" {
			t.Fatalf("Cache-Control = %q, want no-store", got)
		}
	}
}

// Other sites can fire no-cors GETs at 127.0.0.1 in a loop; browsers mark them
// with Sec-Fetch-Site (and CORS requests with Origin), so they must be refused
// before the snapshot runs. Tools that send neither header keep working.
func TestHandlerRejectsCrossSiteRequests(t *testing.T) {
	handler := NewHandler(nil)
	cases := []struct {
		name    string
		headers map[string]string
		want    int
	}{
		{"cross-site", map[string]string{"Sec-Fetch-Site": "cross-site", "Sec-Fetch-Mode": "no-cors"}, http.StatusForbidden},
		{"same-site", map[string]string{"Sec-Fetch-Site": "same-site"}, http.StatusForbidden},
		{"foreign origin", map[string]string{"Origin": "https://evil.example"}, http.StatusForbidden},
		{"null origin", map[string]string{"Origin": "null"}, http.StatusForbidden},
		{"other local origin", map[string]string{"Origin": "http://127.0.0.1:3000"}, http.StatusForbidden},
		{"typed URL", map[string]string{"Sec-Fetch-Site": "none"}, http.StatusOK},
		{"same origin", map[string]string{"Sec-Fetch-Site": "same-origin", "Origin": "http://127.0.0.1:16666"}, http.StatusOK},
		{"curl", nil, http.StatusOK},
	}
	for _, tc := range cases {
		req := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:16666/stats", nil)
		for k, v := range tc.headers {
			req.Header.Set(k, v)
		}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != tc.want {
			t.Fatalf("%s: status = %d, want %d", tc.name, rec.Code, tc.want)
		}
	}
}

func TestStartRefusesNonLoopbackWithoutOptIn(t *testing.T) {
	// All refused before binding, so no port is ever opened off loopback.
	for _, addr := range []string{":0", "0.0.0.0:0", "[::]:0", "192.0.2.1:0"} {
		server, err := Start(addr, nil, Options{})
		if err == nil {
			_ = server.Shutdown(t.Context())
			t.Fatalf("Start(%q) succeeded; want refusal", addr)
		}
		if !errors.Is(err, ErrNotLoopback) {
			t.Fatalf("Start(%q) error = %v; want ErrNotLoopback", addr, err)
		}
	}
	if _, err := Start("127.0.0.1", nil, Options{}); err == nil {
		t.Fatal("Start without a port succeeded; want error")
	}
}

func TestStartLoopbackReportsAndServesGuardedHandler(t *testing.T) {
	for _, tc := range []struct {
		addr string
		opts Options
	}{
		{"127.0.0.1:0", Options{}},
		{"localhost:0", Options{}},
		{"127.0.0.1:0", Options{AllowRemote: true}},
	} {
		server, err := Start(tc.addr, nil, tc.opts)
		if err != nil {
			t.Fatalf("Start(%q): %v", tc.addr, err)
		}
		if !server.Loopback() {
			t.Fatalf("Start(%q).Loopback() = false", tc.addr)
		}
		if _, ok := server.listener.(*limitListener); !ok {
			t.Fatalf("Start(%q) listener is %T; want connection-capped listener", tc.addr, server.listener)
		}

		req, err := http.NewRequest(http.MethodGet, "http://"+server.Addr()+"/healthz", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Host = "rebind.attacker.example"
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET /healthz: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusMisdirectedRequest {
			t.Fatalf("rebinding Host status = %d, want %d", resp.StatusCode, http.StatusMisdirectedRequest)
		}
		if err := server.Shutdown(t.Context()); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	}
}

// Idle connections beyond the cap must wait in the kernel backlog instead of
// each holding a descriptor and a goroutine.
func TestLimitListenerCapsConcurrentConnections(t *testing.T) {
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ln := newLimitListener(inner, 2)
	accepted := make(chan net.Conn, 3)
	go func() {
		defer close(accepted)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			accepted <- conn
		}
	}()

	var clients []net.Conn
	defer func() {
		for _, c := range clients {
			c.Close()
		}
	}()
	for i := 0; i < 3; i++ {
		c, err := net.Dial("tcp", inner.Addr().String())
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		clients = append(clients, c)
	}

	var held []net.Conn
	for i := 0; i < 2; i++ {
		select {
		case c := <-accepted:
			held = append(held, c)
		case <-time.After(5 * time.Second):
			t.Fatalf("connection %d was not accepted", i)
		}
	}
	select {
	case c := <-accepted:
		c.Close()
		t.Fatal("third connection accepted while the cap was full")
	case <-time.After(100 * time.Millisecond):
	}

	// Closing twice must release exactly one slot.
	held[0].Close()
	held[0].Close()
	select {
	case c := <-accepted:
		held = append(held, c)
	case <-time.After(5 * time.Second):
		t.Fatal("waiting connection was not accepted after a slot freed")
	}
	if got := len(ln.(*limitListener).sem); got != 2 {
		t.Fatalf("slots in use = %d, want 2", got)
	}

	if err := ln.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for _, c := range held[1:] {
		c.Close()
	}
	select {
	case _, ok := <-accepted:
		if ok {
			t.Fatal("Accept returned a connection after Close")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Accept did not return after Close")
	}
}
