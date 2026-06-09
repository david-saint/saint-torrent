# Antigravity Developer Preference Memory

- When the user requests to "update the local version" or "rebuild the local version", always run `go install ./cmd/sainttorrent` to update their system-wide Go-installed binary, in addition to building the local binary in the workspace root.
- When verifying Go code changes, always run `go vet ./...` and `go test -race ./...` to check for static analysis issues and race conditions.
