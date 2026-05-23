# Contributing to saintTorrent

Thank you for your interest in contributing to saintTorrent! We welcome community contributions, bug reports, feature requests, and documentation improvements.

---

## Code of Conduct

Please be respectful, collaborative, and constructive when interacting with the project and other contributors.

---

## How Can I Contribute?

### Reporting Bugs
If you find a bug, please check the GitHub issues to see if it has already been reported. If not, open a new issue with:
- A clear description of the bug
- Steps to reproduce it
- Relevant terminal logs or screenshots
- Your operating system and Go version

### Requesting Features
Feel free to open an issue to discuss new features or improvements. Describe the use case and why it would benefit the project.

### Submitting Pull Requests
1. Fork the repository.
2. Create a new topic branch for your changes:
   ```bash
   git checkout -b feature/your-feature-name
   # or for bug fixes:
   git checkout -b fix/your-bug-name
   ```
3. Commit your changes with clear, descriptive commit messages.
4. Push your branch to your fork.
5. Submit a Pull Request (PR) targeting the `main` branch of the original repository.

---

## Development Setup

### Local Setup
Ensure you have Go 1.24+ installed. Clone the repository and run:

```bash
go mod download
```

### Running Tests
We maintain a robust test suite covering rate limiting, session mechanics, and manager functions. Always verify your changes do not break existing code before submitting a PR:

```bash
# Run all unit tests
go test -v ./pkg/downloader/...

# Run with race detector
go test -race -v ./pkg/downloader/...
```

---

## Coding Guidelines

- **Go Standards:** Code must conform to standard Go formatting. Run `go fmt ./...` before committing.
- **Linting:** Ensure your code passes standard golangci-lint or basic go vet rules. Run `go vet ./...` locally.
- **Document Your Code:** Maintain docstrings for exported package symbols, functions, and structs in `pkg/downloader`.
- **Maintain Tests:** If you add new functionality, please add corresponding tests in the appropriate `*_test.go` file.
- **Keep TUI Responsive:** When modifying the main loop in `cmd/sainttorrent/main.go`, ensure that long-running operations are offloaded to background goroutines or bubbles tasks, keeping the Bubble Tea `Update` loop fast and responsive.

---

## License

By contributing to saintTorrent, you agree that your contributions will be licensed under the project's Apache License, Version 2.0.
