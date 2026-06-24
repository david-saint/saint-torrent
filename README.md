# saintTorrent

A beautiful, high-performance BitTorrent client for the terminal, written in Go. saintTorrent utilizes [Bubble Tea](https://github.com/charmbracelet/bubbletea) and [Lip Gloss](https://github.com/charmbracelet/lipgloss) to deliver a gorgeous and feature-rich Terminal User Interface (TUI).

---

## Features

- 🖥️ **Stunning Terminal UI:** Clean layouts, vibrant Dracula-inspired color schemes, and responsive UI elements.
- ⚙️ **Torrent Management:** Add torrents via local `.torrent` file paths or magnet URIs.
- 📊 **Real-time Statistics:** Track download/upload speeds, percent completion, connected peers, and session statistics.
- 🗺️ **Visual Piece Map:** A live, retro piece-map visualizer showing which pieces are completed, downloading, or pending.
- 📂 **Interactive File Explorer:** Browse files inside multi-file torrents and set custom file-level priorities.
- 📶 **DHT Support:** Bootstraps peer discovery via Distributed Hash Tables (DHT) if trackers are unavailable.
- 🛑 **Rate Limiting:** Set global download and upload speed limits directly in the TUI.
- 🔎 **Optional HTTP Stats:** Opt-in JSON stats endpoint for monitoring and headless scripting.

---

## Keyboard Controls

### Dashboard (Torrent List View)
- `q` or `Ctrl+C` - Quit the application and cleanly close all active sessions
- `up`/`down` or `k`/`j` - Navigate the torrent list
- `space` - Pause or resume the selected torrent
- `enter` - Open the detailed view for the selected torrent
- `o` - Open the selected torrent's file/folder location in your file manager (Finder on macOS)
- `a` - Add a new torrent (prompt accepts local filepath or Magnet URI)
- `d` - Set a global download speed limit (in KB/s)
- `u` - Set a global upload speed limit (in KB/s)

### Torrent Details View
- `esc` - Go back to the Torrent List view
- `space` - Pause or resume the torrent
- `f` - Open the File Explorer for this torrent
- `o` - Open this torrent's file/folder location in your file manager (Finder on macOS)
- `q` or `Ctrl+C` - Quit

### File Explorer View
- `esc` - Go back to the Torrent Details view
- `up`/`down` or `k`/`j` - Scroll through the file list
- `space` or `p` - Cycle priority for the selected file (`NORMAL` ➔ `HIGH` ➔ `SKIP`)
- `q` or `Ctrl+C` - Quit

---

## Installation

### Prerequisites
- Go 1.24 or later installed on your system.

### Build from Source
Clone the repository and build the binary:

```bash
git clone https://github.com/david-saint/saint-torrent.git
cd saint-torrent
go build -o sainttorrent ./cmd/sainttorrent
```

---

## Usage

Start the client with default download directory (`.`):

```bash
./sainttorrent
```

Specify a custom download directory:

```bash
./sainttorrent -d /path/to/downloads
```

Start the client and automatically queue a torrent or magnet link:

```bash
./sainttorrent -d /path/to/downloads "magnet:?xt=urn:btih:..."
# or using a local file:
./sainttorrent -d /path/to/downloads my_awesome_file.torrent
```

saintTorrent listens for inbound peers on TCP/UDP port `51413` by default.
The port is stable across runs so it can be forwarded manually, and the client
also attempts automatic UPnP IGD or NAT-PMP mapping:

```bash
./sainttorrent --port 51413
./sainttorrent --no-nat          # keep the stable port, disable automatic mapping
./sainttorrent --port 0          # explicitly request an ephemeral port
```

Peer protocol encryption defaults to `prefer`: saintTorrent tries BitTorrent
MSE/PE first and falls back to plaintext when a peer does not support it. Use
`require` to reject plaintext peer connections, or `disable` for plaintext-only
compatibility:

```bash
./sainttorrent --encryption prefer
./sainttorrent --encryption require
./sainttorrent --encryption disable
```

Storage defaults to regular file-backed downloads. For testing or platform
experiments, select another backend:

```bash
./sainttorrent --storage file   # default persistent files
./sainttorrent --storage mmap   # memory-mapped files, unavailable on Windows
./sainttorrent --storage mem    # in-memory content, not persistent
```

The HTTP stats endpoint is off by default. Enable the read-only JSON API with
`--http-addr`:

```bash
./sainttorrent --http-addr 127.0.0.1:16666
./sainttorrent --headless --http-addr 127.0.0.1:16666
curl http://127.0.0.1:16666/stats
curl http://127.0.0.1:16666/healthz
```

`GET /stats` returns a snapshot of manager limits, listener/NAT ports, aggregate
transfer counters, and per-torrent status, peer, piece, and file stats. The
endpoint does not expose mutating controls; keep it bound to localhost unless
you place it behind your own trusted network or reverse proxy. In headless mode,
forwarded torrent requests that require confirmation are rejected; use
`--no-confirm` when scripting additions into a headless instance.

### macOS Magnet Handler

On macOS you can register saintTorrent as the default handler for `magnet:`
links:

```bash
./register_magnet.sh
```

This builds the CLI, installs a small launcher app to
`~/Applications/saintTorrent.app`, and sets it as the `magnet:` handler. When a
magnet link is opened, the launcher hands it to a running saintTorrent instance
over its IPC socket and focuses the terminal tab waiting for confirmation. If
none is running, it opens a terminal and starts one.

#### Choosing your terminal

The terminal used for that fallback is configurable. Edit
`~/.config/sainttorrent/config.json` (created on first registration) and set
`terminalApp`:

```json
{
  "terminalApp": "iTerm"
}
```

- **`Terminal`** (default), **`iTerm`**, and **`Ghostty`** get first-class focus
  support for a running saintTorrent session. Terminal and iTerm match the
  session by TTY; Ghostty matches saintTorrent's terminal title.
- If no instance is running, Terminal and iTerm are driven directly via
  AppleScript. Other terminals are launched by opening a temporary `.command`
  script with that app. This requires the terminal to support opening `.command`
  documents.

This file is **not** overwritten when you re-run `register_magnet.sh`, so your
choice persists across upgrades.

### Startup, verification & performance

Resumed torrents appear **instantly** on startup: fast-resume state is loaded
without hashing, and each torrent's downloaded pieces are re-verified in the
background (shown as a **Checking** status that settles to Seeding/Downloading
once confirmed). Unverified pieces are never served to peers or counted toward
seeding until they pass the hash check, so corrupt resume data is still caught
and re-downloaded. DHT bootstrapping and the tracker "stopped" announces on quit
are off the critical path, so start and close stay responsive on a slow network.

To measure startup/close time:

```bash
# Per-phase breakdown, printed after the UI exits (also appended to
# $SAINTTORRENT_TIMING_LOG when that variable is set):
SAINTTORRENT_TIMING=1 ./sainttorrent -d /path/to/downloads

# Headless: run the real startup + shutdown without the TUI and print
# startup_ms / shutdown_ms (scriptable with `time`):
SAINTTORRENT_BENCH=1 ./sainttorrent -d /path/to/downloads

# Deterministic micro-benchmarks (cold restore + shutdown):
go test -bench='BenchmarkColdStartup|BenchmarkShutdown' -benchmem ./pkg/downloader
```

---

## Project Structure

```text
├── cmd/
│   └── sainttorrent/         # Main entry point and Bubble Tea TUI
│       └── main.go
├── pkg/
│   └── downloader/           # BitTorrent core implementation
│       ├── manager.go        # Torrent session coordinator
│       ├── session.go        # Peer wire, piece picker, file writer
│       ├── ratelimiter.go    # Bandwidth allocation
│       └── *_test.go         # Comprehensive unit/integration tests
├── go.mod                    # Go dependencies
├── LICENSE                   # Apache License 2.0
└── CONTRIBUTING.md           # Guidelines for contributing
```

---

## License

saintTorrent is released under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
