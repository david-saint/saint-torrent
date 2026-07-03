# Agent Guide — saintTorrent

Shared instructions for every coding agent working in this repo (Claude, Gemini/Antigravity, Codex, …). `CLAUDE.md` and `GEMINI.md` point here — update this file only, so all agents stay in sync.

## Project

saintTorrent is a BitTorrent client in Go: CLI + TUI in `cmd/sainttorrent`, engine in `pkg/` (downloader, storage, tracker, dht, peer, …). Speed comes first — maximum download throughput is the core tenet. Do not put synchronous disk I/O, serialization, or extra lock contention on the hot path (piece completion, block reads/writes, peer loops), and do not add limits or caps that regress throughput.

## Before every commit (mirrors `.github/workflows/ci.yml`)

Run, in this order:

1. `gofmt -l $(git ls-files '*.go')` — must print nothing. CI fails on formatting **before** vet/build/tests, and the compiler and `go vet` do not catch formatting issues such as stray blank lines.
2. `go vet ./...`
3. `go build ./...`
4. `go test -race ./...`

CI additionally runs golangci-lint and a linux/386 build+test (`GOARCH=386 CGO_ENABLED=0` — keep 64-bit atomic fields properly aligned; see `pkg/downloader/atomic_alignment_test.go`).

Enable the repo pre-commit hook once per clone so steps 1–2 run automatically:

```bash
git config core.hooksPath .githooks
```

## Workflow

- Never commit or push directly to `main`, even for small changes. Branch, open a PR (see `CONTRIBUTING.md`), and merge only with green CI.
- "Rebuild" / "update the local version" means running `./rebuild.sh`, which refreshes both the system-wide binary (`go install ./cmd/sainttorrent`, used by the magnet launcher at `~/go/bin/sainttorrent`) and the workspace binary (`./sainttorrent`, gitignored).
