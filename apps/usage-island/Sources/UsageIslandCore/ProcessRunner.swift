import Foundation
import Darwin

public enum ProcessFailure: Error, LocalizedError {
    case timedOut
    case unsuccessful(Int32)
    case outputTooLarge

    public var errorDescription: String? {
        switch self {
        case .timedOut: return "The metrics helper timed out."
        case .unsuccessful(let code): return "The metrics helper exited with status \(code)."
        case .outputTooLarge: return "The metrics helper returned an unexpectedly large response."
        }
    }
}

/// Never waits for a child on the main actor. Output goes to a temporary file,
/// rather than a pipe that can deadlock when a subprocess fills its buffer.
public final class ProcessRunner: @unchecked Sendable {
    private let lock = NSLock()
    private var active: [UUID: Process] = [:]

    public init() {}

    public func run(executable: URL, arguments: [String], environment: [String: String]? = nil,
                    timeout: TimeInterval = 50) async throws -> Data {
        try await Task.detached(priority: .utility) { [self] in
            try blockingRun(executable: executable, arguments: arguments,
                            environment: environment, timeout: timeout)
        }.value
    }

    private func blockingRun(executable: URL, arguments: [String],
                             environment: [String: String]?, timeout: TimeInterval) throws -> Data {
        let temporary = FileManager.default.temporaryDirectory
            .appendingPathComponent("ai-usage-island-\(UUID().uuidString)")
        FileManager.default.createFile(atPath: temporary.path, contents: nil,
                                       attributes: [.posixPermissions: 0o600])
        defer { try? FileManager.default.removeItem(at: temporary) }
        let handle = try FileHandle(forWritingTo: temporary)
        defer { try? handle.close() }
        let process = Process()
        process.executableURL = executable
        process.arguments = arguments
        process.environment = environment
        process.standardOutput = handle
        process.standardError = FileHandle.nullDevice
        let identifier = UUID()
        let finished = DispatchSemaphore(value: 0)
        process.terminationHandler = { _ in finished.signal() }
        try process.run()
        lock.lock()
        active[identifier] = process
        lock.unlock()
        defer {
            lock.lock()
            active.removeValue(forKey: identifier)
            lock.unlock()
        }

        if finished.wait(timeout: .now() + timeout) == .timedOut {
            if process.isRunning { process.terminate() }
            if finished.wait(timeout: .now() + 3) == .timedOut, process.isRunning {
                kill(process.processIdentifier, SIGKILL)
                process.waitUntilExit()
            }
            throw ProcessFailure.timedOut
        }
        guard process.terminationStatus == 0 else {
            throw ProcessFailure.unsuccessful(process.terminationStatus)
        }
        let attributes = try FileManager.default.attributesOfItem(atPath: temporary.path)
        guard ((attributes[.size] as? NSNumber)?.intValue ?? 0) <= 1_000_000 else {
            throw ProcessFailure.outputTooLarge
        }
        return try Data(contentsOf: temporary)
    }

    public func terminateAll() {
        lock.lock()
        let processes = Array(active.values)
        lock.unlock()
        for process in processes where process.isRunning { process.terminate() }
    }
}
