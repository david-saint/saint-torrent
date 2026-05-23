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

---

## Keyboard Controls

### Dashboard (Torrent List View)
- `q` or `Ctrl+C` - Quit the application and cleanly close all active sessions
- `up`/`down` or `k`/`j` - Navigate the torrent list
- `space` - Pause or resume the selected torrent
- `enter` - Open the detailed view for the selected torrent
- `a` - Add a new torrent (prompt accepts local filepath or Magnet URI)
- `d` - Set a global download speed limit (in KB/s)
- `u` - Set a global upload speed limit (in KB/s)

### Torrent Details View
- `esc` - Go back to the Torrent List view
- `space` - Pause or resume the torrent
- `f` - Open the File Explorer for this torrent
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
