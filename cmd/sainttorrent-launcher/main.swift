import Cocoa
import Darwin

struct Config: Decodable {
    let binaryPath: String
    let socketPath: String
    let defaultDownloadDir: String
}

func loadConfig() -> Config {
    guard let url = Bundle.main.url(forResource: "config", withExtension: "json") else {
        return loadFallbackConfig()
    }
    guard let data = try? Data(contentsOf: url) else {
        return loadFallbackConfig()
    }
    do {
        return try JSONDecoder().decode(Config.self, from: data)
    } catch {
        return loadFallbackConfig()
    }
}

func loadFallbackConfig() -> Config {
    let homeDir = NSHomeDirectory()
    let fallbackBinary = "\(homeDir)/go/bin/sainttorrent"
    let fallbackSocket = "\(homeDir)/Library/Application Support/sainttorrent/sainttorrent.sock"
    let fallbackDownload = "\(homeDir)/Downloads"
    return Config(binaryPath: fallbackBinary, socketPath: fallbackSocket, defaultDownloadDir: fallbackDownload)
}

class AppDelegate: NSObject, NSApplicationDelegate {
    func applicationDidFinishLaunching(_ notification: Notification) {
        NSAppleEventManager.shared().setEventHandler(
            self,
            andSelector: #selector(handleGetURLEvent(_:withReplyEvent:)),
            forEventClass: AEEventClass(kInternetEventClass),
            andEventID: AEEventID(kAEGetURL)
        )
    }
    
    @objc func handleGetURLEvent(_ event: NSAppleEventDescriptor, withReplyEvent replyEvent: NSAppleEventDescriptor) {
        if let urlString = event.paramDescriptor(forKeyword: keyDirectObject)?.stringValue {
            handleURL(urlString)
        }
    }
    
    func handleURL(_ urlString: String) {
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
        }
        
        do {
            let decoder = JSONDecoder()
            let response = try decoder.decode(SocketResponse.self, from: responseData)
            if response.status == "ok" {
                NSApp.terminate(nil)
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
        
        let osascriptProcess = Process()
        osascriptProcess.launchPath = "/usr/bin/osascript"
        osascriptProcess.arguments = [
            "-e", "on run argv",
            "-e", "    tell application \"Terminal\"",
            "-e", "        activate",
            "-e", "        do script (item 1 of argv)",
            "-e", "    end tell",
            "-e", "end run",
            "--",
            command
        ]
        osascriptProcess.launch()
        osascriptProcess.waitUntilExit()
        NSApp.terminate(nil)
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
