#!/bin/bash
set -e

# Rebuild saintTorrent: refresh the Go-installed binary (used by the magnet
# launcher at ~/go/bin/sainttorrent) and the local workspace binary.
# Run from anywhere — it operates on the repo this script lives in.

cd "$(dirname "$0")"

echo "Updating system-wide Go-installed binary (~/go/bin/sainttorrent)..."
go install ./cmd/sainttorrent

echo "Building local binary in workspace root (./sainttorrent)..."
go build -o sainttorrent ./cmd/sainttorrent

echo "Done."
