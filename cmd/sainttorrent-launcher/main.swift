import Cocoa
import Darwin

struct Config: Decodable {
    let binaryPath: String
    let socketPath: String
    let defaultDownloadDir: String
    let terminalApp: String
}

// PartialConfig decodes config files tolerantly: every field is optional, so a
// user override file that only sets `terminalApp` (or a stale bundled config
// missing a field) still parses and is overlaid onto the defaults.
struct PartialConfig: Decodable {
    let binaryPath: String?
    let socketPath: String?
    let defaultDownloadDir: String?
    let terminalApp: String?
}

func overlay(base: Config, partial: PartialConfig) -> Config {
    return Config(
        binaryPath: partial.binaryPath ?? base.binaryPath,
        socketPath: partial.socketPath ?? base.socketPath,
        defaultDownloadDir: partial.defaultDownloadDir ?? base.defaultDownloadDir,
        terminalApp: partial.terminalApp ?? base.terminalApp
    )
}

func decodePartialConfig(at path: String) -> PartialConfig? {
    guard let data = FileManager.default.contents(atPath: path) else {
        return nil
    }
    return try? JSONDecoder().decode(PartialConfig.self, from: data)
}

func loadConfig() -> Config {
    // Layer the config: built-in defaults, then the bundled config.json, then
    // the user override at ~/.config/sainttorrent/config.json (terminalApp wins).
    var config = loadFallbackConfig()

    if let url = Bundle.main.url(forResource: "config", withExtension: "json"),
       let bundled = decodePartialConfig(at: url.path) {
        config = overlay(base: config, partial: bundled)
    }

    let userConfigPath = "\(NSHomeDirectory())/.config/sainttorrent/config.json"
    if let userConfig = decodePartialConfig(at: userConfigPath) {
        config = overlay(base: config, partial: userConfig)
    }

    return config
}

func loadFallbackConfig() -> Config {
    let homeDir = NSHomeDirectory()
    let fallbackBinary = "\(homeDir)/go/bin/sainttorrent"
    let fallbackSocket = "\(homeDir)/.config/sainttorrent/sainttorrent.sock"
    let fallbackDownload = "\(homeDir)/Downloads"
    return Config(binaryPath: fallbackBinary, socketPath: fallbackSocket, defaultDownloadDir: fallbackDownload, terminalApp: "Terminal")
}

class AppDelegate: NSObject, NSApplicationDelegate {
    private let maxStartingRetries = 120
    private let startingRetryDelay = 0.25

    func applicationWillFinishLaunching(_ notification: Notification) {
        NSAppleEventManager.shared().setEventHandler(
            self,
            andSelector: #selector(handleGetURLEvent(_:withReplyEvent:)),
            forEventClass: AEEventClass(kInternetEventClass),
            andEventID: AEEventID(kAEGetURL)
        )
    }
    
    @objc func handleGetURLEvent(_ event: NSAppleEventDescriptor, withReplyEvent replyEvent: NSAppleEventDescriptor) {
        if let urlString = event.paramDescriptor(forKeyword: keyDirectObject)?.stringValue {
            handleURL(urlString, startingRetry: 0)
        }
    }
    
    func handleURL(_ urlString: String, startingRetry: Int) {
        let config = loadConfig()
        
        // Construct the framed JSON message using JSONSerialization for safe escaping
        let messageDict: [String: Any] = [
            "items": [urlString],
            "confirm": true,
            "download_dir": config.defaultDownloadDir
        ]
        
        guard let jsonData = try? JSONSerialization.data(withJSONObject: messageDict, options: []),
              let jsonString = String(data: jsonData, encoding: .utf8) else {
            NSApp.terminate(nil)
            return
        }
        
        // Attempt Unix socket connection using native Darwin APIs
        let socketFd = socket(PF_LOCAL, SOCK_STREAM, 0)
        if socketFd < 0 {
            fallbackToTerminal(config: config, urlString: urlString)
            return
        }
        
        // Set SO_NOSIGPIPE to prevent app termination when writing to closed sockets
        var optval: Int32 = 1
        if setsockopt(socketFd, SOL_SOCKET, SO_NOSIGPIPE, &optval, socklen_t(MemoryLayout<Int32>.size)) < 0 {
            close(socketFd)
            fallbackToTerminal(config: config, urlString: urlString)
            return
        }
        
        // Configure receive and send timeouts for socket operations (5 seconds)
        var timeout = timeval(tv_sec: 5, tv_usec: 0)
        if setsockopt(socketFd, SOL_SOCKET, SO_RCVTIMEO, &timeout, socklen_t(MemoryLayout<timeval>.size)) < 0 {
            close(socketFd)
            fallbackToTerminal(config: config, urlString: urlString)
            return
        }
        if setsockopt(socketFd, SOL_SOCKET, SO_SNDTIMEO, &timeout, socklen_t(MemoryLayout<timeval>.size)) < 0 {
            close(socketFd)
            fallbackToTerminal(config: config, urlString: urlString)
            return
        }
        
        // Set socket to nonblocking mode
        let flags = fcntl(socketFd, F_GETFL, 0)
        if flags < 0 || fcntl(socketFd, F_SETFL, flags | O_NONBLOCK) < 0 {
            close(socketFd)
            fallbackToTerminal(config: config, urlString: urlString)
            return
        }
        
        // Setup unix domain address
        var addr = sockaddr_un()
        addr.sun_len = UInt8(MemoryLayout<sockaddr_un>.size)
        addr.sun_family = sa_family_t(AF_LOCAL)
        let pathBytes = config.socketPath.utf8CString
        let maxPathLen = 104
        withUnsafeMutablePointer(to: &addr.sun_path) { sunPathPtr in
            let rawPtr = UnsafeMutableRawPointer(sunPathPtr).assumingMemoryBound(to: CChar.self)
            for i in 0..<min(pathBytes.count, maxPathLen) {
                rawPtr[i] = pathBytes[i]
            }
            if pathBytes.count < maxPathLen {
                rawPtr[pathBytes.count] = 0
            } else {
                rawPtr[maxPathLen - 1] = 0
            }
        }
        
        let addrSize = MemoryLayout<sockaddr_un>.size
        let connectResult = withUnsafePointer(to: &addr) { addrPtr in
            addrPtr.withMemoryRebound(to: sockaddr.self, capacity: 1) { sockaddrPtr in
                connect(socketFd, sockaddrPtr, socklen_t(addrSize))
            }
        }
        
        if connectResult < 0 {
            let err = errno
            if err != EINPROGRESS {
                close(socketFd)
                fallbackToTerminal(config: config, urlString: urlString)
                return
            }
        }
        
        // Wait up to 6 seconds for connect to succeed using poll
        var pollFd = pollfd(fd: socketFd, events: Int16(POLLOUT), revents: 0)
        let pollResult = poll(&pollFd, 1, 6000)
        if pollResult <= 0 {
            close(socketFd)
            fallbackToTerminal(config: config, urlString: urlString)
            return
        }
        
        // Check socket error status
        var socketError: Int32 = 0
        var errorLen = socklen_t(MemoryLayout<Int32>.size)
        let getOptionResult = getsockopt(socketFd, SOL_SOCKET, SO_ERROR, &socketError, &errorLen)
        if getOptionResult < 0 || socketError != 0 {
            close(socketFd)
            fallbackToTerminal(config: config, urlString: urlString)
            return
        }
        
        // Restore socket to blocking mode for data read/write
        if fcntl(socketFd, F_SETFL, flags) < 0 {
            close(socketFd)
            fallbackToTerminal(config: config, urlString: urlString)
            return
        }
        
        // Send JSON data + \n
        let payload = jsonString + "\n"
        let payloadData = payload.data(using: .utf8)!
        let payloadBytes = [UInt8](payloadData)
        var totalWritten = 0
        while totalWritten < payloadBytes.count {
            var written = -1
            repeat {
                written = payloadBytes.withUnsafeBytes { bufferPtr -> Int in
                    let ptr = bufferPtr.baseAddress!.advanced(by: totalWritten)
                    return write(socketFd, ptr, payloadBytes.count - totalWritten)
                }
            } while written < 0 && errno == EINTR
            
            if written <= 0 {
                close(socketFd)
                showNSAlertAndExit(message: "Failed to write payload to active saintTorrent instance.")
            }
            totalWritten += written
        }
        
        // Read response until newline (maximum 65537 bytes)
        var readBuf = [UInt8]()
        var tempByte: UInt8 = 0
        var reachedNewline = false
        
        while readBuf.count < 65537 {
            var bytesRead = -1
            repeat {
                bytesRead = read(socketFd, &tempByte, 1)
            } while bytesRead < 0 && errno == EINTR
            
            if bytesRead < 0 {
                close(socketFd)
                showNSAlertAndExit(message: "Failed to read response from active saintTorrent instance.")
            } else if bytesRead == 0 {
                // EOF reached
                if reachedNewline {
                    break
                } else {
                    close(socketFd)
                    showNSAlertAndExit(message: "Malformed socket response: EOF reached before newline.")
                }
            } else {
                readBuf.append(tempByte)
                if tempByte == UInt8(ascii: "\n") {
                    reachedNewline = true
                    break
                }
            }
        }
        
        if !reachedNewline {
            close(socketFd)
            showNSAlertAndExit(message: "Malformed socket response: Response exceeded 65,537 bytes limit.")
        }
        
        close(socketFd)
        
        let responseData = Data(readBuf)
        
        struct SocketResponse: Decodable {
            let status: String
            let message: String?
            let terminalTTY: String?
            let terminalProgram: String?
            let terminalTitle: String?

            enum CodingKeys: String, CodingKey {
                case status
                case message
                case terminalTTY = "terminal_tty"
                case terminalProgram = "terminal_program"
                case terminalTitle = "terminal_title"
            }
        }
        
        do {
            let decoder = JSONDecoder()
            let response = try decoder.decode(SocketResponse.self, from: responseData)
            if response.status == "ok" {
                focusRunningTerminal(
                    config: config,
                    tty: response.terminalTTY,
                    terminalProgram: response.terminalProgram,
                    terminalTitle: response.terminalTitle
                )
                NSApp.terminate(nil)
            } else if response.status == "starting" {
                if startingRetry >= maxStartingRetries {
                    showNSAlertAndExit(message: "Timed out waiting for saintTorrent to finish starting.")
                }
                DispatchQueue.main.asyncAfter(deadline: .now() + startingRetryDelay) { [weak self] in
                    self?.handleURL(urlString, startingRetry: startingRetry + 1)
                }
            } else {
                let errMsg = response.message ?? "Unknown error"
                showNSAlertAndExit(message: "saintTorrent running instance reported an error: \(errMsg)")
            }
        } catch {
            showNSAlertAndExit(message: "Malformed JSON response from active saintTorrent instance.")
        }
    }
    
    func fallbackToTerminal(config: Config, urlString: String) {
        let escapedBinary = escapeForShell(config.binaryPath)
        let escapedDir = escapeForShell(config.defaultDownloadDir)
        let escapedURL = escapeForShell(urlString)

        let command = "exec \(escapedBinary) -d \(escapedDir) --confirm \(escapedURL)"

        let app = config.terminalApp.trimmingCharacters(in: .whitespacesAndNewlines)
        switch app.lowercased() {
        case "", "terminal", "terminal.app":
            launchInTerminalApp(command: command)
        case "iterm", "iterm2", "iterm.app":
            launchInITerm(command: command)
        default:
            launchInGenericTerminal(appName: app, command: command)
        }
        NSApp.terminate(nil)
    }

    func focusRunningTerminal(
        config: Config,
        tty: String?,
        terminalProgram: String?,
        terminalTitle: String?
    ) {
        let configuredApp = config.terminalApp.trimmingCharacters(in: .whitespacesAndNewlines)
        let app: String

        switch terminalProgram?.lowercased() {
        case "apple_terminal":
            app = "Terminal"
        case "iterm.app", "iterm2":
            app = "iTerm"
        case "ghostty":
            app = "Ghostty"
        default:
            app = configuredApp.isEmpty ? "Terminal" : configuredApp
        }

        let targetTTY = tty ?? ""
        switch app.lowercased() {
        case "terminal", "terminal.app":
            focusTerminalApp(tty: targetTTY)
        case "iterm", "iterm2", "iterm.app":
            focusITerm(tty: targetTTY)
        case "ghostty", "ghostty.app":
            focusGhostty(title: terminalTitle ?? "saintTorrent")
        default:
            activateApplication(named: app)
        }
    }

    func focusTerminalApp(tty: String) {
        let focused = runAppleScript(arguments: [
            "-e", "on run argv",
            "-e", "    set targetTTY to item 1 of argv",
            "-e", "    tell application \"Terminal\"",
            "-e", "        if targetTTY is not \"\" then",
            "-e", "            repeat with terminalWindow in windows",
            "-e", "                repeat with terminalTab in tabs of terminalWindow",
            "-e", "                    if (tty of terminalTab as text) is targetTTY then",
            "-e", "                        set selected of terminalTab to true",
            "-e", "                        set miniaturized of terminalWindow to false",
            "-e", "                        set index of terminalWindow to 1",
            "-e", "                        activate",
            "-e", "                        return",
            "-e", "                    end if",
            "-e", "                end repeat",
            "-e", "            end repeat",
            "-e", "        end if",
            "-e", "        activate",
            "-e", "    end tell",
            "-e", "end run",
            "--",
            tty
        ])
        if !focused {
            activateApplication(named: "Terminal")
        }
    }

    func focusGhostty(title: String) {
        let focused = runAppleScript(arguments: [
            "-e", "on run argv",
            "-e", "    set targetTitle to item 1 of argv",
            "-e", "    tell application \"Ghostty\"",
            "-e", "        repeat with terminalWindow in windows",
            "-e", "            repeat with terminalTab in tabs of terminalWindow",
            "-e", "                repeat with terminalSession in terminals of terminalTab",
            "-e", "                    if (name of terminalSession as text) is targetTitle then",
            "-e", "                        focus (contents of terminalSession)",
            "-e", "                        return",
            "-e", "                    end if",
            "-e", "                end repeat",
            "-e", "            end repeat",
            "-e", "        end repeat",
            "-e", "        activate",
            "-e", "    end tell",
            "-e", "end run",
            "--",
            title
        ])
        if !focused {
            activateApplication(named: "Ghostty")
        }
    }

    func focusITerm(tty: String) {
        let focused = runAppleScript(arguments: [
            "-e", "on run argv",
            "-e", "    set targetTTY to item 1 of argv",
            "-e", "    tell application \"iTerm\"",
            "-e", "        if targetTTY is not \"\" then",
            "-e", "            repeat with terminalWindow in windows",
            "-e", "                repeat with terminalTab in tabs of terminalWindow",
            "-e", "                    repeat with terminalSession in sessions of terminalTab",
            "-e", "                        if (tty of terminalSession as text) is targetTTY then",
            "-e", "                            tell terminalSession to select",
            "-e", "                            tell terminalTab to select",
            "-e", "                            tell terminalWindow to select",
            "-e", "                            activate",
            "-e", "                            return",
            "-e", "                        end if",
            "-e", "                    end repeat",
            "-e", "                end repeat",
            "-e", "            end repeat",
            "-e", "        end if",
            "-e", "        activate",
            "-e", "    end tell",
            "-e", "end run",
            "--",
            tty
        ])
        if !focused {
            activateApplication(named: "iTerm")
        }
    }

    func runAppleScript(arguments: [String]) -> Bool {
        let osascriptProcess = Process()
        osascriptProcess.launchPath = "/usr/bin/osascript"
        osascriptProcess.arguments = arguments
        osascriptProcess.launch()
        osascriptProcess.waitUntilExit()
        return osascriptProcess.terminationStatus == 0
    }

    func activateApplication(named appName: String) {
        let openProcess = Process()
        openProcess.launchPath = "/usr/bin/open"
        openProcess.arguments = ["-a", appName]
        openProcess.launch()
        openProcess.waitUntilExit()
    }

    func launchInTerminalApp(command: String) {
        let osascriptProcess = Process()
        osascriptProcess.launchPath = "/usr/bin/osascript"
        osascriptProcess.arguments = [
            "-e", "on run argv",
            "-e", "    set terminalWasRunning to application \"Terminal\" is running",
            "-e", "    tell application \"Terminal\"",
            "-e", "        if terminalWasRunning then",
            "-e", "            do script (item 1 of argv)",
            "-e", "        else",
            "-e", "            activate",
            "-e", "            repeat 100 times",
            "-e", "                if (count of windows) > 0 then exit repeat",
            "-e", "                delay 0.05",
            "-e", "            end repeat",
            "-e", "            if (count of windows) > 0 then",
            "-e", "                do script (item 1 of argv) in front window",
            "-e", "            else",
            "-e", "                do script (item 1 of argv)",
            "-e", "            end if",
            "-e", "        end if",
            "-e", "        activate",
            "-e", "    end tell",
            "-e", "end run",
            "--",
            command
        ]
        osascriptProcess.launch()
        osascriptProcess.waitUntilExit()
    }

    func launchInITerm(command: String) {
        let osascriptProcess = Process()
        osascriptProcess.launchPath = "/usr/bin/osascript"
        osascriptProcess.arguments = [
            "-e", "on run argv",
            "-e", "    tell application \"iTerm\"",
            "-e", "        activate",
            "-e", "        set newWindow to (create window with default profile)",
            "-e", "        tell current session of newWindow",
            "-e", "            write text (item 1 of argv)",
            "-e", "        end tell",
            "-e", "    end tell",
            "-e", "end run",
            "--",
            command
        ]
        osascriptProcess.launch()
        osascriptProcess.waitUntilExit()
    }

    // Generic fallback for terminals without dedicated AppleScript support: write
    // a self-deleting .command script and ask the named app to open it. This only
    // runs the command in terminals registered to handle .command documents.
    func launchInGenericTerminal(appName: String, command: String) {
        let scriptBody = "#!/bin/bash\nrm -f -- \"$0\"\n\(command)\n"
        let tmpDir = NSTemporaryDirectory() as NSString
        let scriptPath = tmpDir.appendingPathComponent("sainttorrent-\(UUID().uuidString).command")

        do {
            try scriptBody.write(toFile: scriptPath, atomically: true, encoding: .utf8)
        } catch {
            // Fall back to Terminal.app if we can't stage the script.
            launchInTerminalApp(command: command)
            return
        }
        chmod(scriptPath, 0o700)

        let openProcess = Process()
        openProcess.launchPath = "/usr/bin/open"
        openProcess.arguments = ["-a", appName, scriptPath]
        openProcess.launch()
        openProcess.waitUntilExit()
    }

    func escapeForShell(_ s: String) -> String {
        return "'" + s.replacingOccurrences(of: "'", with: "'\\''") + "'"
    }
    
    func showNSAlertAndExit(message: String) -> Never {
        let alert = NSAlert()
        alert.messageText = "saintTorrent Alert"
        alert.informativeText = message
        alert.alertStyle = .critical
        alert.addButton(withTitle: "OK")
        
        NSApp.activate(ignoringOtherApps: true)
        alert.runModal()
        exit(0)
    }
}

let app = NSApplication.shared
let delegate = AppDelegate()
app.delegate = delegate
app.run()
