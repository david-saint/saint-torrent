package downloader

import (
	"context"
	"testing"
	"time"
)

func TestNATPMPAdvertisesGrantedPortAndDeletesWithZeroLifetime(t *testing.T) {
	client := &fakePMPClient{grant: 40001}
	mapper := &natpmpMapper{client: client}
	ctx := context.Background()

	// The gateway may grant a port other than the one asked for.
	port, err := mapper.AddPortMapping(ctx, "tcp", 51413, 50000, natMappingDescription, natMappingLifetime)
	if err != nil {
		t.Fatalf("AddPortMapping: %v", err)
	}
	if port != 40001 {
		t.Fatalf("expected the granted port 40001, got %d", port)
	}
	if err := mapper.DeletePortMapping(ctx, "tcp", 51413, port); err != nil {
		t.Fatalf("DeletePortMapping: %v", err)
	}
	if err := mapper.DeletePortMapping(ctx, "udp", 51413, port); err != nil {
		t.Fatalf("DeletePortMapping: %v", err)
	}

	client.mu.Lock()
	defer client.mu.Unlock()
	want := []string{"tcp 51413 50000 1800", "tcp 51413 0 0", "udp 51413 0 0"}
	if len(client.calls) != len(want) {
		t.Fatalf("expected requests %v, got %v", want, client.calls)
	}
	for i := range want {
		if client.calls[i] != want[i] {
			t.Fatalf("expected requests %v, got %v", want, client.calls)
		}
	}
}

func TestNATPMPRejectsZeroGrantedPort(t *testing.T) {
	mapper := &natpmpMapper{client: &fakePMPClient{}}
	if port, err := mapper.AddPortMapping(context.Background(), "tcp", 51413, 0,
		natMappingDescription, natMappingLifetime); err == nil {
		t.Fatalf("accepted external port %d", port)
	}
}

func TestNATPMPCallStopsAtContext(t *testing.T) {
	block := make(chan struct{})
	defer close(block)
	mapper := &natpmpMapper{client: &fakePMPClient{block: block, grant: 40001}}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := mapper.AddPortMapping(ctx, "tcp", 51413, 0, natMappingDescription, natMappingLifetime); err == nil {
		t.Fatal("a gateway that never answers produced a mapping")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("AddPortMapping outlived its context by %v", elapsed)
	}
}
