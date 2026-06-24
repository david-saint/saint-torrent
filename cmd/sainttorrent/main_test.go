package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"sainttorrent/pkg/downloader"
	"sainttorrent/pkg/mse"
	"sainttorrent/pkg/storage"
)

func TestGetSpaceActionHelp(t *testing.T) {
	tests := []struct {
		isPaused    bool
		isCompleted bool
		want        string
	}{
		{isPaused: false, isCompleted: false, want: "Pause"},
		{isPaused: true, isCompleted: false, want: "Resume"},
		{isPaused: false, isCompleted: true, want: "Stop Seeding"},
		{isPaused: true, isCompleted: true, want: "Start Seeding"},
	}

	for _, tt := range tests {
		got := getSpaceActionHelp(tt.isPaused, tt.isCompleted)
		if got != tt.want {
			t.Errorf("getSpaceActionHelp(paused=%v, completed=%v) = %q; want %q",
				tt.isPaused, tt.isCompleted, got, tt.want)
		}
	}
}

func TestParseCLIArgsNetworkingDefaultsAndOverrides(t *testing.T) {
	defaults := parseCLIArgs(nil)
	if defaults.listenPort != defaultPeerPort || defaults.httpAddr != "" || defaults.headless || !defaults.natEnabled || defaults.encryption != mse.PolicyPrefer || defaults.storage != storage.BackendFile || defaults.err != nil {
		t.Fatalf("unexpected networking defaults: %+v", defaults)
	}

	overrides := parseCLIArgs([]string{"--port", "52000", "--http-addr", "127.0.0.1:16666", "--headless", "--no-nat", "--encryption", "require", "--storage", "mmap"})
	if overrides.listenPort != 52000 || overrides.httpAddr != "127.0.0.1:16666" || !overrides.headless || overrides.natEnabled || overrides.encryption != mse.PolicyRequire || overrides.storage != storage.BackendMMap || overrides.err != nil {
		t.Fatalf("unexpected networking overrides: %+v", overrides)
	}

	invalid := parseCLIArgs([]string{"--port", "70000"})
	if invalid.err == nil {
		t.Fatal("expected invalid port error")
	}

	missingHTTPAddr := parseCLIArgs([]string{"--http-addr"})
	if missingHTTPAddr.err == nil {
		t.Fatal("expected missing HTTP address error")
	}

	invalidEncryption := parseCLIArgs([]string{"--encryption", "bogus"})
	if invalidEncryption.err == nil {
		t.Fatal("expected invalid encryption policy error")
	}

	invalidStorage := parseCLIArgs([]string{"--storage", "bogus"})
	if invalidStorage.err == nil {
		t.Fatal("expected invalid storage backend error")
	}
}

func TestGetIndicator(t *testing.T) {
	tests := []struct {
		isPaused    bool
		isCompleted bool
		want        string
	}{
		{isPaused: false, isCompleted: false, want: "▶"},
		{isPaused: true, isCompleted: false, want: "⏸"},
		{isPaused: false, isCompleted: true, want: "▶"},
		{isPaused: true, isCompleted: true, want: "⏹"},
	}

	for _, tt := range tests {
		got := getIndicator(tt.isPaused, tt.isCompleted)
		if got != tt.want {
			t.Errorf("getIndicator(paused=%v, completed=%v) = %q; want %q",
				tt.isPaused, tt.isCompleted, got, tt.want)
		}
	}
}

func TestGetSpeedStr(t *testing.T) {
	tests := []struct {
		isPaused    bool
		isCompleted bool
		speed       float64
		want        string
	}{
		{isPaused: true, isCompleted: false, speed: 0, want: "paused"},
		{isPaused: true, isCompleted: true, speed: 0, want: "stopped"},
		{isPaused: false, isCompleted: false, speed: 0, want: "↓ 0 B/s"},
		{isPaused: false, isCompleted: false, speed: 1024, want: "↓ 1.0 KB/s"},
		{isPaused: false, isCompleted: true, speed: 50 * 1024, want: "↑ 50.0 KB/s"},
	}

	for _, tt := range tests {
		got := getSpeedStr(tt.isPaused, tt.isCompleted, tt.speed)
		// Strip extra spaces or check strings.Contains to deal with alignment formatting
		if tt.isPaused {
			if got != tt.want {
				t.Errorf("getSpeedStr(paused=%v, completed=%v, speed=%f) = %q; want %q",
					tt.isPaused, tt.isCompleted, tt.speed, got, tt.want)
			}
		} else {
			direction := "↓"
			if tt.isCompleted {
				direction = "↑"
			}
			wantSpeed := strings.TrimSpace(strings.TrimPrefix(tt.want, direction))
			if !strings.Contains(got, direction) || !strings.Contains(got, wantSpeed) {
				t.Errorf("getSpeedStr(paused=%v, completed=%v, speed=%f) = %q; want containing %q",
					tt.isPaused, tt.isCompleted, tt.speed, got, tt.want)
			}
		}
	}
}

func TestDeleteViewFlow(t *testing.T) {
	// Initialize a dummy model
	mgr := downloader.NewTorrentManager()
	defer mgr.Close()
	m := initialModel(mgr, ".", "", nil)
	m.viewMode = viewDetail
	m.deleteWithFiles = false
	m.deleteErr = nil

	// 1. Pressing "x" in viewDetail transitions to viewDeleteConfirm (delete task only)
	mUpdated, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("x")})
	m = mUpdated.(model)
	if m.viewMode != viewDeleteConfirm {
		t.Errorf("expected viewMode to be viewDeleteConfirm, got %v", m.viewMode)
	}
	if m.deleteWithFiles {
		t.Error("expected deleteWithFiles to be false on 'x'")
	}
	if m.deleteErr != nil {
		t.Errorf("expected deleteErr to be nil, got %v", m.deleteErr)
	}

	// 2. Pressing "esc" in viewDeleteConfirm transitions back to viewDetail (no error)
	mUpdated, _ = m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	m = mUpdated.(model)
	if m.viewMode != viewDetail {
		t.Errorf("expected viewMode to be viewDetail after cancel, got %v", m.viewMode)
	}

	// 3. Pressing "X" in viewDetail transitions to viewDeleteConfirm (delete task & files)
	mUpdated, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("X")})
	m = mUpdated.(model)
	if m.viewMode != viewDeleteConfirm {
		t.Errorf("expected viewMode to be viewDeleteConfirm, got %v", m.viewMode)
	}
	if !m.deleteWithFiles {
		t.Error("expected deleteWithFiles to be true on 'X'")
	}

	// 4. If deleteErr is present, pressing "esc" transitions to viewList
	m.deleteErr = fmt.Errorf("some deletion error")
	mUpdated, _ = m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	m = mUpdated.(model)
	if m.viewMode != viewList {
		t.Errorf("expected viewMode to be viewList after error escape, got %v", m.viewMode)
	}
	if m.deleteErr != nil {
		t.Error("expected deleteErr to be cleared after transitioning back to list")
	}
}

func TestDeleteConfirmationRunsInBackground(t *testing.T) {
	mgr := downloader.NewTorrentManager()
	defer mgr.Close()

	const infoHashHex = "542e85596f7a0dd05eefdb78b0ac1736496f8626"
	_, err := mgr.AddMagnet("magnet:?xt=urn:btih:"+infoHashHex+"&dn=BackgroundDelete", t.TempDir())
	if err != nil {
		t.Fatalf("failed to add session: %v", err)
	}

	m := initialModel(mgr, ".", "", nil)
	m.viewMode = viewDetail

	mUpdated, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("x")})
	m = mUpdated.(model)
	mUpdated, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("y")})
	m = mUpdated.(model)

	if cmd == nil {
		t.Fatal("expected deletion to return a background command")
	}
	if !m.deleteInProgress {
		t.Fatal("expected deletion progress state immediately after confirmation")
	}
	if mgr.GetSession(infoHashHex) == nil {
		t.Fatal("session was removed synchronously before the background command ran")
	}
	if view := m.View(); !strings.Contains(view, "Deletion is running in the background") {
		t.Fatalf("expected deletion progress view, got:\n%s", view)
	}
	mUpdated, duplicateCmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("y")})
	m = mUpdated.(model)
	if duplicateCmd != nil {
		t.Fatal("expected repeated confirmation to be ignored while deletion is running")
	}

	msg := cmd()
	mUpdated, _ = m.Update(msg)
	m = mUpdated.(model)

	if m.deleteInProgress {
		t.Fatal("expected deletion progress state to clear after completion")
	}
	if m.viewMode != viewList {
		t.Fatalf("expected list view after deletion, got %v", m.viewMode)
	}
	if mgr.GetSession(infoHashHex) != nil {
		t.Fatal("expected session to be removed after background command completed")
	}
}

func TestConfirmViewFlow(t *testing.T) {
	mgr := downloader.NewTorrentManager()
	defer mgr.Close()

	_, err := mgr.AddMagnet("magnet:?xt=urn:btih:542e85596f7a0dd05eefdb78b0ac1736496f8626&dn=TestMortalKombat", ".")
	if err != nil {
		t.Fatalf("failed to add existing magnet: %v", err)
	}

	item1 := "magnet:?xt=urn:btih:542e85596f7a0dd05eefdb78b0ac1736496f8626&dn=TestMortalKombat"
	name1, hashHex1, err1 := parseItem(item1)
	if err1 != nil {
		t.Fatalf("failed to parse item 1: %v", err1)
	}

	item2 := "magnet:?xt=urn:btih:642e85596f7a0dd05eefdb78b0ac1736496f8626&dn=NewTorrent"
	name2, hashHex2, err2 := parseItem(item2)
	if err2 != nil {
		t.Fatalf("failed to parse item 2: %v", err2)
	}

	pending := []pendingItem{
		{
			rawURL:      item1,
			displayName: name1,
			infoHashHex: hashHex1,
			downloadDir: ".",
			isDuplicate: mgr.GetSession(hashHex1) != nil,
		},
		{
			rawURL:      item2,
			displayName: name2,
			infoHashHex: hashHex2,
			downloadDir: ".",
			isDuplicate: mgr.GetSession(hashHex2) != nil,
		},
	}

	m := initialModel(mgr, ".", "", pending)
	if m.viewMode != viewAddConfirm {
		t.Errorf("expected viewMode to be viewAddConfirm, got %v", m.viewMode)
	}
	if !m.pendingItems[0].isDuplicate {
		t.Error("expected first item to be marked as duplicate")
	}
	if m.pendingItems[1].isDuplicate {
		t.Error("expected second item to not be marked as duplicate")
	}

	mUpdated, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("n")})
	m = mUpdated.(model)
	if m.viewMode != viewAddConfirm {
		t.Errorf("expected viewMode to still be viewAddConfirm after skipping first item, got %v", m.viewMode)
	}
	if m.pendingIdx != 1 {
		t.Errorf("expected pendingIdx to be 1, got %d", m.pendingIdx)
	}

	mUpdated, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("y")})
	m = mUpdated.(model)
	if m.viewMode != viewList {
		t.Errorf("expected viewMode to transition to viewList after second item, got %v", m.viewMode)
	}

	sess := mgr.GetSession(hashHex2)
	if sess == nil {
		t.Error("expected second torrent session to be created after confirmation")
	}
}

func TestModalQueueing(t *testing.T) {
	mgr := downloader.NewTorrentManager()
	defer mgr.Close()

	m := initialModel(mgr, ".", "", nil)

	msg := addTorrentMsg{
		msg: socketMessage{
			Items:   []string{"magnet:?xt=urn:btih:542e85596f7a0dd05eefdb78b0ac1736496f8626&dn=TestMortalKombat"},
			Confirm: true,
		},
	}
	mUpdated, _ := m.Update(msg)
	m = mUpdated.(model)

	if m.viewMode != viewAddConfirm {
		t.Errorf("expected immediate transition to viewAddConfirm, got %v", m.viewMode)
	}
	if len(m.pendingItems) != 1 {
		t.Errorf("expected 1 pending item, got %d", len(m.pendingItems))
	}

	m.viewMode = viewInput

	mUpdated, _ = m.Update(msg)
	m = mUpdated.(model)

	if m.viewMode != viewInput {
		t.Errorf("expected viewMode to remain viewInput, got %v", m.viewMode)
	}
	if len(m.pendingItems) != 2 {
		t.Errorf("expected 2 pending items, got %d", len(m.pendingItems))
	}

	mUpdated, _ = m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	m = mUpdated.(model)
	if m.viewMode != viewAddConfirm {
		t.Errorf("expected transition to viewAddConfirm after modal exit, got %v", m.viewMode)
	}
}

func TestSanitizeText(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{input: "Normal Name", want: "Normal Name"},
		{input: "Name\u0000With\rControl\nChars", want: "Name With Control Chars"},
		{input: "Name\u0080With\u009fC1Controls", want: "Name With C1Controls"},
		{input: "   Spaces Trimmed   ", want: "Spaces Trimmed"},
	}

	for _, tt := range tests {
		got := sanitizeText(tt.input)
		if got != tt.want {
			t.Errorf("sanitizeText(%q) = %q; want %q", tt.input, got, tt.want)
		}
	}
}

func TestLockHeldAndSocketNotReadyRetry(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sainttorrent-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	lockPath := filepath.Join(tmpDir, "sainttorrent.lock")
	socketPath := filepath.Join(tmpDir, "sainttorrent.sock")

	lockFile, lockErr := acquireLock(lockPath)
	if lockErr != nil {
		t.Fatalf("failed to acquire lock: %v", lockErr)
	}
	defer lockFile.Close()

	go func() {
		time.Sleep(100 * time.Millisecond)
		ln, err := net.Listen("unix", socketPath)
		if err == nil {
			defer ln.Close()
			conn, err := ln.Accept()
			if err == nil {
				conn.Close()
			}
		}
	}()

	var conn net.Conn
	var connErr error
	for retry := 0; retry < 5; retry++ {
		conn, connErr = net.Dial("unix", socketPath)
		if connErr == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if connErr != nil {
		t.Errorf("expected secondary to eventually connect to the socket, but failed: %v", connErr)
	} else {
		conn.Close()
	}
}

func TestForwardingWithConfirmFalse(t *testing.T) {
	mgr := downloader.NewTorrentManager()
	defer mgr.Close()

	m := initialModel(mgr, ".", "", nil)

	msg := addTorrentMsg{
		msg: socketMessage{
			Items:   []string{"magnet:?xt=urn:btih:542e85596f7a0dd05eefdb78b0ac1736496f8626&dn=TestImmediate"},
			Confirm: false,
		},
	}
	mUpdated, _ := m.Update(msg)
	m = mUpdated.(model)

	if m.viewMode != viewList {
		t.Errorf("expected viewMode to remain viewList, got %v", m.viewMode)
	}

	sess := mgr.GetSession("542e85596f7a0dd05eefdb78b0ac1736496f8626")
	if sess == nil {
		t.Error("expected session to be added immediately")
	}
}

func TestWriteConfigSubprocess(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sainttorrent-config-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	outputPath := filepath.Join(tmpDir, "config.json")

	if os.Getenv("BE_CRASH_TEST") == "1" {
		out := os.Getenv("BE_CRASH_TEST_OUTPUT")
		os.Args = []string{"sainttorrent", "--write-config", out}
		main()
		return
	}

	// Spawn subprocess
	cmd := exec.Command(os.Args[0], "-test.run=TestWriteConfigSubprocess")
	cmd.Env = append(os.Environ(), "BE_CRASH_TEST=1", "BE_CRASH_TEST_OUTPUT="+outputPath, "SAINTTORRENT_IPC_DIR="+tmpDir)
	err = cmd.Run()
	if err != nil {
		t.Fatalf("Subprocess failed: %v", err)
	}

	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read output config: %v", err)
	}

	var cfg struct {
		BinaryPath         string `json:"binaryPath"`
		SocketPath         string `json:"socketPath"`
		DefaultDownloadDir string `json:"defaultDownloadDir"`
		TerminalApp        string `json:"terminalApp"`
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatalf("Failed to unmarshal output config: %v", err)
	}

	if cfg.SocketPath == "" || cfg.DefaultDownloadDir == "" {
		t.Errorf("Config values empty: %+v", cfg)
	}

	if cfg.TerminalApp != "Terminal" {
		t.Errorf("Expected default terminalApp %q, got %q", "Terminal", cfg.TerminalApp)
	}
}

func TestHandleSocketConnection(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	mgr := downloader.NewTorrentManager()
	defer mgr.Close()

	// Run tea.Program in background to process messages sent via p.Send
	var discard io.Writer = os.NewFile(0, os.DevNull)
	if f, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0); err == nil {
		discard = f
		defer f.Close()
	}
	p := tea.NewProgram(initialModel(mgr, ".", "", nil), tea.WithInput(nil), tea.WithOutput(discard))
	programMu.Lock()
	teaProgram = p
	programMu.Unlock()
	defer func() {
		programMu.Lock()
		teaProgram = nil
		programMu.Unlock()
	}()
	go func() {
		_, _ = p.Run()
	}()
	defer p.Quit()

	var handlersWG sync.WaitGroup
	handlersWG.Add(1)
	shutdownChan := make(chan struct{})

	go func() {
		handleSocketConnection(serverConn, shutdownChan, mgr, &handlersWG, terminalIdentity{
			TTY:     "/dev/ttys042",
			Program: "Apple_Terminal",
			Title:   terminalWindowTitle,
		}, false)
	}()

	msg := socketMessage{
		Items:   []string{"magnet:?xt=urn:btih:542e85596f7a0dd05eefdb78b0ac1736496f8626&dn=TestMortalKombat"},
		Confirm: true,
	}
	data, _ := json.Marshal(msg)
	_, _ = clientConn.Write(append(data, '\n'))

	buf := make([]byte, 1024)
	n, err := clientConn.Read(buf)
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}

	var resp socketResponse
	if err := json.Unmarshal(buf[:n], &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	// TUI processes this successfully, so we expect status == "ok"
	if resp.Status != "ok" {
		t.Errorf("expected status=ok, got status=%s message=%s", resp.Status, resp.Message)
	}
	if resp.TerminalTTY != "/dev/ttys042" ||
		resp.TerminalProgram != "Apple_Terminal" ||
		resp.TerminalTitle != terminalWindowTitle {
		t.Errorf("unexpected terminal identity in response: %+v", resp)
	}
}

func TestHandleSocketConnectionHeadlessNoConfirm(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	mgr := downloader.NewTorrentManager()
	defer mgr.Close()

	programMu.Lock()
	teaProgram = nil
	programMu.Unlock()

	var handlersWG sync.WaitGroup
	handlersWG.Add(1)
	shutdownChan := make(chan struct{})

	go func() {
		handleSocketConnection(serverConn, shutdownChan, mgr, &handlersWG, terminalIdentity{}, true)
	}()

	const infoHash = "542e85596f7a0dd05eefdb78b0ac1736496f8626"
	msg := socketMessage{
		Items:       []string{"magnet:?xt=urn:btih:" + infoHash + "&dn=HeadlessNoConfirm"},
		Confirm:     false,
		DownloadDir: t.TempDir(),
	}
	data, _ := json.Marshal(msg)
	_, _ = clientConn.Write(append(data, '\n'))

	buf := make([]byte, 1024)
	n, err := clientConn.Read(buf)
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}

	var resp socketResponse
	if err := json.Unmarshal(buf[:n], &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp.Status != "ok" {
		t.Fatalf("expected status=ok, got status=%s message=%s", resp.Status, resp.Message)
	}
	if mgr.GetSession(infoHash) == nil {
		t.Fatal("expected headless socket handler to add torrent immediately")
	}

	handlersWG.Wait()
}

func TestSocketLimitsAndMalformed(t *testing.T) {
	mgr := downloader.NewTorrentManager()
	defer mgr.Close()

	p := tea.NewProgram(initialModel(mgr, ".", "", nil), tea.WithInput(nil), tea.WithOutput(io.Discard))
	programMu.Lock()
	teaProgram = p
	programMu.Unlock()
	defer func() {
		programMu.Lock()
		teaProgram = nil
		programMu.Unlock()
	}()
	go func() { _, _ = p.Run() }()
	defer p.Quit()

	// 1. Frame too large
	serverConn1, clientConn1 := net.Pipe()
	var handlersWG sync.WaitGroup
	handlersWG.Add(1)
	shutdownChan := make(chan struct{})

	go func() {
		handleSocketConnection(serverConn1, shutdownChan, mgr, &handlersWG, terminalIdentity{}, false)
	}()

	largeData := make([]byte, 70000)
	for i := range largeData {
		largeData[i] = 'a'
	}

	go func() {
		_, _ = clientConn1.Write(largeData)
	}()

	buf := make([]byte, 1024)
	n, err := clientConn1.Read(buf)
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}
	var resp socketResponse
	if err := json.Unmarshal(buf[:n], &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp.Status != "error" || !strings.Contains(resp.Message, "too large") {
		t.Errorf("expected frame too large error, got %s: %s", resp.Status, resp.Message)
	}
	clientConn1.Close()
	serverConn1.Close()
	handlersWG.Wait()

	// 2. EOF before \n
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create malformed-frame listener: %v", err)
	}
	defer listener.Close()

	handlersWG.Add(1)
	go func() {
		serverConn2, acceptErr := listener.Accept()
		if acceptErr != nil {
			handlersWG.Done()
			return
		}
		handleSocketConnection(serverConn2, shutdownChan, mgr, &handlersWG, terminalIdentity{}, false)
	}()

	rawClientConn2, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial malformed-frame listener: %v", err)
	}
	clientConn2 := rawClientConn2.(*net.TCPConn)
	defer clientConn2.Close()

	if _, err := clientConn2.Write([]byte(`{"items":["test"]}`)); err != nil {
		t.Fatalf("failed to write malformed frame: %v", err)
	}
	if err := clientConn2.CloseWrite(); err != nil {
		t.Fatalf("failed to half-close malformed frame connection: %v", err)
	}
	if err := clientConn2.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("failed to set malformed-frame read deadline: %v", err)
	}
	n, err = clientConn2.Read(buf)
	if err != nil {
		t.Fatalf("failed to read malformed-frame response: %v", err)
	}
	var resp2 socketResponse
	if err := json.Unmarshal(buf[:n], &resp2); err != nil {
		t.Fatalf("failed to unmarshal malformed-frame response: %v", err)
	}
	if resp2.Status != "error" || !strings.Contains(resp2.Message, "EOF") {
		t.Errorf("expected EOF framing error, got %s: %s", resp2.Status, resp2.Message)
	}
	handlersWG.Wait()
}

func TestSocketPartialFailures(t *testing.T) {
	mgr := downloader.NewTorrentManager()
	defer mgr.Close()

	p := tea.NewProgram(initialModel(mgr, ".", "", nil), tea.WithInput(nil), tea.WithOutput(io.Discard))
	programMu.Lock()
	teaProgram = p
	programMu.Unlock()
	defer func() {
		programMu.Lock()
		teaProgram = nil
		programMu.Unlock()
	}()
	go func() { _, _ = p.Run() }()
	defer p.Quit()

	serverConn, clientConn := net.Pipe()
	var handlersWG sync.WaitGroup
	handlersWG.Add(1)
	shutdownChan := make(chan struct{})

	go func() {
		handleSocketConnection(serverConn, shutdownChan, mgr, &handlersWG, terminalIdentity{}, false)
	}()

	msg := socketMessage{
		Items:   []string{"magnet:?xt=urn:btih:542e85596f7a0dd05eefdb78b0ac1736496f8626&dn=ValidMagnet", ""},
		Confirm: false,
	}
	data, _ := json.Marshal(msg)
	go func() {
		_, _ = clientConn.Write(append(data, '\n'))
	}()

	buf := make([]byte, 1024)
	n, err := clientConn.Read(buf)
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}

	var resp socketResponse
	if err := json.Unmarshal(buf[:n], &resp); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if resp.Status != "error" {
		t.Errorf("expected status=error due to partial failure, got status=%s message=%s", resp.Status, resp.Message)
	}

	sess := mgr.GetSession("542e85596f7a0dd05eefdb78b0ac1736496f8626")
	if sess == nil {
		t.Error("expected valid torrent session to be created despite partial failure in batch")
	}

	serverConn.Close()
	clientConn.Close()
	handlersWG.Wait()
}

func TestSocketShutdownUnblocksRead(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	mgr := downloader.NewTorrentManager()
	defer mgr.Close()

	p := tea.NewProgram(initialModel(mgr, ".", "", nil), tea.WithInput(nil), tea.WithOutput(io.Discard))
	programMu.Lock()
	teaProgram = p
	programMu.Unlock()
	defer func() {
		programMu.Lock()
		teaProgram = nil
		programMu.Unlock()
	}()
	go func() { _, _ = p.Run() }()
	defer p.Quit()

	registerConn(serverConn)

	var handlersWG sync.WaitGroup
	handlersWG.Add(1)
	shutdownChan := make(chan struct{})

	handlerDone := make(chan struct{})
	go func() {
		handleSocketConnection(serverConn, shutdownChan, mgr, &handlersWG, terminalIdentity{}, false)
		close(handlerDone)
	}()

	closeActiveConns()

	select {
	case <-handlerDone:
		// Success
	case <-time.After(1 * time.Second):
		t.Error("timed out waiting for socket handler to exit after closeActiveConns")
	}
	handlersWG.Wait()
}

func TestCustomConfigSingleInstance(t *testing.T) {
	if os.Getenv("BE_SINGLE_INSTANCE_PRIMARY") == "1" {
		configDir := os.Getenv("PRIMARY_CONFIG_DIR")
		opts := parseCLIArgs([]string{"-c", configDir})
		if opts.configDir != configDir {
			os.Exit(6)
		}

		ipcDir, err := resolveIPCDir()
		if err != nil {
			os.Exit(1)
		}
		lockPath := filepath.Join(ipcDir, "sainttorrent.lock")
		socketPath := filepath.Join(ipcDir, "sainttorrent.sock")

		lockFile, err := acquireLock(lockPath)
		if err != nil {
			os.Exit(2)
		}
		defer lockFile.Close()

		_ = os.Remove(socketPath)
		listener, err := net.Listen("unix", socketPath)
		if err != nil {
			os.Exit(3)
		}
		defer listener.Close()

		// Accept one connection, read the payload, write it to the output file, and respond "ok"
		conn, err := listener.Accept()
		if err != nil {
			os.Exit(4)
		}
		defer conn.Close()

		var payload []byte
		buf := make([]byte, 1)
		for {
			n, err := conn.Read(buf)
			if err != nil {
				break
			}
			if n > 0 {
				if buf[0] == '\n' {
					break
				}
				payload = append(payload, buf[0])
			}
		}

		outPath := os.Getenv("PRIMARY_OUT_FILE")
		_ = os.WriteFile(outPath, payload, 0644)

		resp := socketResponse{Status: "ok", Message: "handled"}
		respBytes, _ := json.Marshal(resp)
		_ = writeFrame(conn, respBytes)
		os.Exit(0)
	}

	if os.Getenv("BE_SINGLE_INSTANCE_SECONDARY") == "1" {
		configDir := os.Getenv("SECONDARY_CONFIG_DIR")
		magnetURL := os.Getenv("SECONDARY_MAGNET")
		os.Args = []string{"sainttorrent", "-c", configDir, magnetURL}
		main()
		return
	}

	tmpDir, err := os.MkdirTemp("", "ipc-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ipcDir := filepath.Join(tmpDir, "ipc")
	dir1 := filepath.Join(tmpDir, "config1")
	dir2 := filepath.Join(tmpDir, "config2")
	_ = os.MkdirAll(ipcDir, 0700)
	_ = os.MkdirAll(dir1, 0700)
	_ = os.MkdirAll(dir2, 0700)

	outFile := filepath.Join(tmpDir, "received.json")

	// 1. Start primary process
	cmdPrimary := exec.Command(os.Args[0], "-test.run=TestCustomConfigSingleInstance")
	cmdPrimary.Env = append(os.Environ(),
		"BE_SINGLE_INSTANCE_PRIMARY=1",
		"SAINTTORRENT_IPC_DIR="+ipcDir,
		"PRIMARY_CONFIG_DIR="+dir1,
		"PRIMARY_OUT_FILE="+outFile,
	)
	if err := cmdPrimary.Start(); err != nil {
		t.Fatalf("failed to start primary: %v", err)
	}
	defer func() {
		_ = cmdPrimary.Process.Kill()
	}()

	time.Sleep(100 * time.Millisecond)

	// 2. Start secondary process running the actual main()!
	// It should fail to lock, connect to the socket, forward the item, get "ok", and exit 0.
	cmdSecondary := exec.Command(os.Args[0], "-test.run=TestCustomConfigSingleInstance")
	cmdSecondary.Env = append(os.Environ(),
		"BE_SINGLE_INSTANCE_SECONDARY=1",
		"SAINTTORRENT_IPC_DIR="+ipcDir,
		"SECONDARY_CONFIG_DIR="+dir2,
		"SECONDARY_MAGNET=magnet:?xt=urn:btih:542e85596f7a0dd05eefdb78b0ac1736496f8626&dn=ForwardedTest",
	)

	out, err := cmdSecondary.CombinedOutput()
	if err != nil {
		t.Fatalf("secondary process failed: %v. Output:\n%s", err, string(out))
	}

	var data []byte
	for i := 0; i < 20; i++ {
		data, err = os.ReadFile(outFile)
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("failed to read forwarded data file: %v", err)
	}

	var msg socketMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("failed to unmarshal forwarded data: %v", err)
	}

	if len(msg.Items) != 1 || msg.Items[0] != "magnet:?xt=urn:btih:542e85596f7a0dd05eefdb78b0ac1736496f8626&dn=ForwardedTest" {
		t.Errorf("unexpected forwarded items: %+v", msg.Items)
	}
}

func TestForwardedDownloadDir(t *testing.T) {
	mgr := downloader.NewTorrentManager()
	defer mgr.Close()

	m := initialModel(mgr, "/default/dir", "", nil)
	msg := addTorrentMsg{
		msg: socketMessage{
			Items:       []string{"magnet:?xt=urn:btih:542e85596f7a0dd05eefdb78b0ac1736496f8626&dn=TestMortalKombat"},
			Confirm:     true,
			DownloadDir: "/custom/dir",
		},
	}
	mUpdated, _ := m.Update(msg)
	m = mUpdated.(model)

	if len(m.pendingItems) != 1 {
		t.Fatalf("expected 1 pending item, got %d", len(m.pendingItems))
	}
	if m.pendingItems[0].downloadDir != "/custom/dir" {
		t.Errorf("expected downloadDir to be /custom/dir, got %s", m.pendingItems[0].downloadDir)
	}

	// Test case where Confirm is false (added immediately)
	msgNoConfirm := addTorrentMsg{
		msg: socketMessage{
			Items:       []string{"magnet:?xt=urn:btih:642e85596f7a0dd05eefdb78b0ac1736496f8626&dn=TestNoConfirm"},
			Confirm:     false,
			DownloadDir: "/custom/dir/immediate",
		},
	}
	mUpdated, _ = m.Update(msgNoConfirm)
	m = mUpdated.(model)

	sess := mgr.GetSession("642e85596f7a0dd05eefdb78b0ac1736496f8626")
	if sess == nil {
		t.Fatalf("expected session to be added")
	}
	gotDir := sess.DownloadDir()
	expectedImmediateDir, err := filepath.Abs("/custom/dir/immediate")
	if err != nil {
		expectedImmediateDir = "/custom/dir/immediate"
	}
	if gotDir != expectedImmediateDir {
		t.Errorf("expected session download dir to be %s, got %s", expectedImmediateDir, gotDir)
	}
}

func TestRelativeTorrentPathNormalization(t *testing.T) {
	files := []string{"magnet:?xt=urn:btih:542e85596f7a0dd05eefdb78b0ac1736496f8626", "some/relative/path.torrent"}

	normalized := normalizeForwardedItems(files)

	if normalized[0] != files[0] {
		t.Errorf("magnet link should not be changed, got %s", normalized[0])
	}
	if !filepath.IsAbs(normalized[1]) {
		t.Errorf("relative path should be normalized to absolute, got %s", normalized[1])
	}
	expectedAbs, _ := filepath.Abs("some/relative/path.torrent")
	if normalized[1] != expectedAbs {
		t.Errorf("expected %s, got %s", expectedAbs, normalized[1])
	}
}

func TestCompletedQueueReset(t *testing.T) {
	mgr := downloader.NewTorrentManager()
	defer mgr.Close()

	pending := []pendingItem{
		{
			rawURL:      "magnet:?xt=urn:btih:542e85596f7a0dd05eefdb78b0ac1736496f8626&dn=TestMortalKombat",
			displayName: "Test",
			infoHashHex: "542e85596f7a0dd05eefdb78b0ac1736496f8626",
			downloadDir: ".",
		},
	}

	m := initialModel(mgr, ".", "", pending)
	if m.viewMode != viewAddConfirm {
		t.Fatalf("expected viewMode to be viewAddConfirm, got %v", m.viewMode)
	}

	mUpdated, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("y")})
	m = mUpdated.(model)

	if m.viewMode != viewList {
		t.Errorf("expected viewMode to transition to viewList, got %v", m.viewMode)
	}
	if m.pendingItems != nil {
		t.Errorf("expected pendingItems to be reset to nil, got %v", m.pendingItems)
	}
	if m.pendingIdx != 0 {
		t.Errorf("expected pendingIdx to be reset to 0, got %d", m.pendingIdx)
	}
}

func TestConfirmationAddErrors(t *testing.T) {
	mgr := downloader.NewTorrentManager()
	defer mgr.Close()

	pending := []pendingItem{
		{
			rawURL:      "invalid_file_path.torrent",
			displayName: "Invalid Torrent",
			infoHashHex: "1234567890abcdef1234567890abcdef12345678",
			downloadDir: ".",
		},
	}

	m := initialModel(mgr, ".", "", pending)

	mUpdated, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("y")})
	m = mUpdated.(model)

	if m.addConfirmErr == nil {
		t.Fatal("expected addConfirmErr to be set, but got nil")
	}
	if m.viewMode != viewAddConfirm {
		t.Errorf("expected viewMode to remain viewAddConfirm on error, got %v", m.viewMode)
	}
	if m.pendingIdx != 0 {
		t.Errorf("expected pendingIdx to remain 0 on error, got %d", m.pendingIdx)
	}

	renderedView := m.View()
	if !strings.Contains(renderedView, "Error adding torrent") {
		t.Errorf("expected view to contain error message, got:\n%s", renderedView)
	}

	mUpdated, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("y")})
	m = mUpdated.(model)

	if m.addConfirmErr != nil {
		t.Errorf("expected addConfirmErr to be cleared, got %v", m.addConfirmErr)
	}
	if m.viewMode != viewList {
		t.Errorf("expected viewMode to transition to viewList, got %v", m.viewMode)
	}
}

func TestPausedDuplicateResumption(t *testing.T) {
	mgr := downloader.NewTorrentManager()
	defer mgr.Close()

	sess, err := mgr.AddMagnet("magnet:?xt=urn:btih:542e85596f7a0dd05eefdb78b0ac1736496f8626&dn=TestDuplicate", ".")
	if err != nil {
		t.Fatalf("failed to add session: %v", err)
	}
	sess.Pause()

	if !sess.IsPaused() {
		t.Fatal("expected session to be paused")
	}

	pending := []pendingItem{
		{
			rawURL:      "magnet:?xt=urn:btih:542e85596f7a0dd05eefdb78b0ac1736496f8626&dn=TestDuplicate",
			displayName: "TestDuplicate",
			infoHashHex: "542e85596f7a0dd05eefdb78b0ac1736496f8626",
			downloadDir: ".",
			isDuplicate: true,
		},
	}

	m := initialModel(mgr, ".", "", pending)

	mUpdated, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("y")})
	_ = mUpdated.(model)

	if sess.IsPaused() {
		t.Error("expected duplicate session to be resumed, but it is still paused")
	}
}

func TestLockErrors(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "lock-err-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	lockPath := filepath.Join(tmpDir, "sainttorrent.lock")
	lockFile1, err := acquireLock(lockPath)
	if err != nil {
		t.Fatalf("failed to acquire lock 1: %v", err)
	}
	defer lockFile1.Close()

	_, err2 := acquireLock(lockPath)
	if err2 == nil {
		t.Fatal("expected lock contention error, got nil")
	}
	if !errors.Is(err2, errLockContention) {
		t.Errorf("expected errors.Is(err, errLockContention) to be true, got %v", err2)
	}

	badLockPath := filepath.Join(tmpDir, "non_existent_dir", "sainttorrent.lock")
	_, err3 := acquireLock(badLockPath)
	if err3 == nil {
		t.Fatal("expected lock error on invalid path, got nil")
	}
	if errors.Is(err3, errLockContention) {
		t.Error("expected fatal error to not be errLockContention")
	}
}
