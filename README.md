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
- `x` - Delete the selected torrent task (keeps downloaded files)
- `X` - Delete the selected torrent task and its downloaded files

### Torrent Details View
- `esc` - Go back to the Torrent List view
- `space` - Pause or resume the torrent
- `f` - Open the File Explorer for this torrent
- `o` - Open this torrent's file/folder location in your file manager (Finder on macOS)
- `x` - Delete the torrent task (keeps downloaded files)
- `X` - Delete the torrent task and its downloaded files
- `q` or `Ctrl+C` - Quit

### File Explorer View
- `esc` - Go back to the Torrent Details view
- `up`/`down` or `k`/`j` - Scroll through the file list
- `space` or `p` - Cycle priority for the selected file (`NORMAL` ➔ `HIGH` ➔ `SKIP`)
- `q` or `Ctrl+C` - Quit

---

## Installation

### Prerequisites
- Go 1.26.8 or later (older Go installs with `GOTOOLCHAIN=auto`, the default, fetch it automatically; the floor keeps known standard-library security fixes in every build).

### Build from Source
Clone the repository and build the binary:

```bash
git clone https://github.com/david-saint/saint-torrent.git
cd saint-torrent
go build -o sainttorrent ./cmd/sainttorrent
```

To stamp a version into the binary (reported by `--version`):

```bash
go build -ldflags "-X main.version=v1.0.0" -o sainttorrent ./cmd/sainttorrent
```

---

## Usage

List all available flags or print the version:

```bash
./sainttorrent --help
./sainttorrent --version
```

Start the client with the configured download directory (`~/Downloads` when no
override is configured):

```bash
./sainttorrent
```

Specify a custom download directory:

```bash
./sainttorrent -d /path/to/downloads
```

Add one or more fallback directories in priority order. saintTorrent checks the
preferred directory and then each fallback whenever a torrent is added:

```bash
./sainttorrent -d "/Volumes/SAINT SSD/saintTorrent" \
  --fallback-dir ~/Downloads
```

The selected directory must be creatable and pass a durable write test. On
macOS, an unavailable `/Volumes/<name>` path is never recreated on the internal
disk. If a removable drive is reconnected while saintTorrent is running, new
torrents prefer it again. Existing torrents stay bound to the directory where
they were added; saintTorrent does not split or migrate an active torrent after
a drive disconnects.

The CLI also reads `defaultDownloadDir` and `fallbackDownloadDirs` from
`~/.config/sainttorrent/config.json`. When `--config <path>` is supplied, it
reads `<path>/config.json` instead. Explicit `--dir` and `--fallback-dir` flags
override their corresponding configured values.

If an existing config file is unreadable or malformed, saintTorrent exits
without adding torrents rather than silently reverting to another directory.

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

Debug logging is off by default. For field troubleshooting, enable structured
JSON-lines logging to a rotating file with either `SAINTTORRENT_LOG` or
`--log`:

```bash
SAINTTORRENT_LOG="$HOME/Library/Logs/sainttorrent/debug.log" ./sainttorrent   # macOS
./sainttorrent --log ~/.cache/sainttorrent/debug.log --log-level debug        # Linux
```

Log levels are `debug`, `info`, `warn`, and `error`. Logs can include local
paths, torrent names, and peer addresses, so keep them in a private per-user
directory rather than a shared one such as `/tmp`. Missing parent directories
are created owner-only (`0700`). On Unix-like systems the log file is
owner-readable only, and saintTorrent refuses to open a log path that is a
symlink, a hard link, or a file owned by another user. Rotation defaults to
10 MiB with 3 backups and can be tuned with `SAINTTORRENT_LOG_MAX_SIZE` (for
example `25mb`) and a positive `SAINTTORRENT_LOG_MAX_BACKUPS`.

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
endpoint does not expose mutating controls, but it has no authentication and
reveals torrent names, local paths, and peer addresses, so it only binds to a
loopback address by default. Binding a LAN or wildcard address such as
`0.0.0.0:16666` requires `--http-allow-remote`; the startup line then shows a
warning. The bound address is shown on the startup line in both the TUI and
headless mode.

To resist DNS rebinding, the API answers only requests whose `Host` is an IP
literal, `localhost`, or the host given to `--http-addr` (others get `421`),
and it rejects browser requests from other sites (`Sec-Fetch-Site` of
`cross-site`/`same-site`, or a foreign `Origin`) with `403`. `curl`, scripts,
and a URL typed into the browser are unaffected. A reverse proxy in front of it
must forward a `Host` of `127.0.0.1` or `localhost` (nginx's default does). At
most 64 connections are served at once.

In headless mode, forwarded torrent requests that require confirmation are
rejected; use `--no-confirm` when scripting additions into a headless instance.

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

The CLI and magnet launcher share download-directory defaults from this file.
For example:

```json
{
  "terminalApp": "Terminal",
  "defaultDownloadDir": "/Volumes/SAINT SSD/saintTorrent",
  "fallbackDownloadDirs": ["/Users/your-name/Downloads"]
}
```

`fallbackDownloadDirs` is ordered and may contain more than one path.

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

### State, crashes and recovery

saintTorrent keeps its state in `os.UserConfigDir()/sainttorrent`
(`~/.config/sainttorrent` on Linux, `~/Library/Application Support/sainttorrent`
on macOS), or in the directory given to `--config`. saintTorrent creates what it
keeps there private to your user (directories `0700`, files `0600`):

| Path | What it holds |
| --- | --- |
| `session.json` | Every torrent: download directory, paused state, file priorities, and any crash quarantine |
| `torrents/` | A cached copy of each `.torrent` (these can carry private-tracker passkeys) |
| `restore-failures.log` | Why a torrent failed to restore at startup, one line per failure |
| `crash/` | One `<time>-<info-hash>-<component>.txt` file per recorded crash (the newest 32 are kept), and `fatal.txt`, where the Go runtime writes fatal errors (rotated to `fatal.txt.1` past 1 MiB) |
| `running` | Present while saintTorrent runs; removed on a clean exit |
| `crash-state.json` | How many runs in a row ended in a crash no torrent was blamed for |

Pass `--no-persist` to keep no state at all: nothing is restored on the next
launch, and no crash handling applies.

When a goroutine working for a torrent panics (a peer connection, a tracker
announce, piece writing or checking, a web seed, restoring it at startup),
saintTorrent records the crash in `crash/` under that torrent's info-hash and
exits with the full trace, rather than carrying on in a state it cannot trust.
On the next launch a leftover `running` file shows that the previous run did
not exit cleanly, and:

- A torrent a crash was recorded for is restored **Paused after crash**
  (quarantined). It is not checked or started, since its data or its peers may
  be what crashed; its error line points at the crash file. Resuming it lifts
  the quarantine. Until then it stays quarantined across restarts.
- A torrent that crashed saintTorrent *while being restored* is not loaded at
  all, since loading it would crash again. It stays in `session.json`, the
  startup line names its info-hash, and `--start-paused` loads it paused and
  quarantined.
- A crash no torrent is blamed for (the terminal UI, the DHT, a fatal runtime
  error, or the process being killed outright by `SIGKILL`, the OOM killer or
  a power cut) in the first 10 minutes of a run is counted. After two such crashes in a row, every torrent is
  restored paused, and the startup line says `saintTorrent stopped
  unexpectedly twice in a row; all torrents were restored paused`. A clean
  exit, or a run that lasted 10 minutes, resets the count. Quitting with `q`,
  `Ctrl+C`, `SIGTERM`, or closing the terminal window (`SIGHUP`) is a clean
  exit.

Start with `--start-paused` to restore every torrent paused regardless, for
example to get past a crash loop and resume torrents one at a time.

---

## Known limitations

- **SHA-1 only (BitTorrent v1).** Pieces and info-hashes are SHA-1. Its
  collision attacks (SHAttered, and BitErrant against BitTorrent) let the author
  of a torrent build two payloads that share piece hashes, so only the person
  who made the torrent can exploit them, not other peers. v2 torrents (BEP 52,
  SHA-256) are not supported.
- **DHT lookups are not private.** A DHT lookup sends the torrent's real
  info-hash to the nodes it queries, so they learn what you are downloading.
- **Peer IDs are linkable within a torrent.** The peer ID starts with
  `-ST0001-`, which identifies the client, and stays the same for every
  connection of a torrent, so peers can link those connections to each other.
  Each torrent gets its own random peer ID.

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

### Startup checking and fast resume

The file and mmap backends restore completed downloads from a durable resume
checkpoint after validating each file's size, modification time, and identity.
Identity always includes the change timestamp, which moves even when a tool
restores the modification time after an in-place edit; the mmap backend releases
its mappings before taking a checkpoint so that timestamp is settled before it is
recorded. Unchanged files need no content reads. Files that changed are rechecked
along with any torrent pieces crossing their boundaries; unaffected files retain
their verified state, and pieces the checkpoint never claimed stay immediately
downloadable instead of queueing behind a hash.

Older state files contain completion hints, so the first launch after upgrading
performs one verification pass before saving the new checkpoint. Interrupted
checks never promote unverified hints, and the cheap hints written while a check
is running replay the previous checkpoint rather than replacing it. Completed,
paused and closing sessions flush the files they wrote and atomically replace
resume state from background persistence work, outside peer and piece locks; a
file nothing has written to since the last checkpoint is never re-flushed.

Two trade-offs are deliberate. Changing a file's metadata without changing its
contents — a `chmod`, a new extended attribute, a hardlink, or remounting the
volume — moves the change timestamp, so those files are hashed once on the next
launch. And a file the client is itself writing cannot be compared against its own
previous metadata, so an external in-place edit of an already-completed region of
a file that is still downloading is not detected until the next full check.

Use `sainttorrent --recheck` to force full hashing of the torrents restored on a
launch, including their unchanged files. Metadata validation is a fast-resume
policy, not a replacement for a full integrity check when silent corruption is
suspected. Checking progress, disk read speed, and ETA appear separately from
network transfer statistics; paused torrents still display their checking status.

Run `go test -run '^$' -bench BenchmarkResumeVerification -benchmem ./pkg/downloader`
to compare completed-torrent startup with a full verification pass. This benchmark
includes verification and completion persistence, unlike `BenchmarkColdStartup`.
Its small fixture is normally cached; use real cold files on the target disk when
measuring physical checking throughput.
