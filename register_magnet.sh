#!/bin/bash
set -e

echo "Updating system-wide Go-installed binary..."
go install ./cmd/sainttorrent

echo "Building local binary in workspace root..."
go build -o sainttorrent ./cmd/sainttorrent

# Resolve absolute path to the installed binary
BINARY_PATH=$(command -v sainttorrent || true)
if [ -z "$BINARY_PATH" ] || [[ "$BINARY_PATH" != /* ]]; then
    if [ -n "$GOBIN" ] && [ -f "$GOBIN/sainttorrent" ]; then
        BINARY_PATH="$GOBIN/sainttorrent"
    else
        GOPATH_VAL=$(go env GOPATH)
        BINARY_PATH="$GOPATH_VAL/bin/sainttorrent"
    fi
fi

if [ ! -f "$BINARY_PATH" ]; then
    echo "Error: Could not locate installed sainttorrent binary at $BINARY_PATH"
    exit 1
fi
echo "Resolved sainttorrent binary path: $BINARY_PATH"

# Setup directories
CONFIG_DIR="$HOME/.config/sainttorrent"
SOCKET_PATH="$CONFIG_DIR/sainttorrent.sock"
DEFAULT_DOWNLOAD_DIR="$HOME/Downloads"
mkdir -p "$CONFIG_DIR"
chmod 700 "$CONFIG_DIR"

# Seed a persistent, user-editable override config (only if it doesn't exist, so
# manual edits are never clobbered on re-registration). The launcher reads this
# file and lets its terminalApp value override the bundled default.
USER_CONFIG="$CONFIG_DIR/config.json"
if [ ! -f "$USER_CONFIG" ]; then
    printf '{\n  "terminalApp": "Terminal"\n}\n' > "$USER_CONFIG"
    echo "Created user config at $USER_CONFIG"
    echo "  Set \"terminalApp\" there to your preferred terminal (e.g. \"iTerm\", \"Ghostty\")."
fi

mkdir -p "$HOME/Applications"
APP_DIR="$HOME/Applications/saintTorrent.app"
mkdir -p "$APP_DIR/Contents/MacOS"
mkdir -p "$APP_DIR/Contents/Resources"

# Compile native macOS App wrapper
echo "Compiling native macOS App wrapper..."
swiftc -O -framework Cocoa cmd/sainttorrent-launcher/main.swift -o "$APP_DIR/Contents/MacOS/saintTorrentLauncher"

# Generate config.json inside Resources directory using --write-config
echo "Generating config.json..."
"$BINARY_PATH" --write-config "$APP_DIR/Contents/Resources/config.json"

echo "Configuring App Bundle Info.plist..."
cp cmd/sainttorrent-launcher/Info.plist "$APP_DIR/Contents/Info.plist"

# Verify Info.plist syntax
plutil -lint "$APP_DIR/Contents/Info.plist"

# Sign App Bundle locally (ad-hoc signature)
echo "Signing application bundle..."
codesign -s - --force "$APP_DIR"

# Verify code signature
codesign --verify --verbose "$APP_DIR"

# Register with Launch Services
echo "Registering application bundle with Launch Services..."
/System/Library/Frameworks/CoreServices.framework/Frameworks/LaunchServices.framework/Support/lsregister -f "$APP_DIR"

# Set default URL handler for magnet scheme
echo "Setting saintTorrent as default handler for 'magnet' scheme..."
swift -e 'import Foundation; import CoreServices; LSSetDefaultHandlerForURLScheme("magnet" as CFString, "com.sainttorrent.client" as CFString)'

echo "saintTorrent has been successfully registered to handle 'magnet:' URL schemes!"
