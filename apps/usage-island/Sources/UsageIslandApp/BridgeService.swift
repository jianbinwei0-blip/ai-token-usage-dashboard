import Foundation
import UsageIslandCore

struct BridgeService: Sendable {
    let runner: ProcessRunner
    let python: URL
    let script: URL

    init(runner: ProcessRunner = ProcessRunner()) throws {
        self.runner = runner
        let environment = ProcessInfo.processInfo.environment
        let candidates = [environment["AI_USAGE_PYTHON"], "/opt/homebrew/bin/python3",
                          "/usr/local/bin/python3", "/usr/bin/python3"].compactMap { $0 }
        guard let executable = candidates.first(where: FileManager.default.isExecutableFile(atPath:)) else {
            throw BridgeError.pythonMissing
        }
        python = URL(fileURLWithPath: executable)

        // SwiftPM's generated accessor otherwise falls back to a build-directory
        // path. Resolve the installed resource bundle explicitly so the app works
        // with Documents unavailable, and after the build directory is removed.
        var directories: [URL] = []
        if let override = environment["AI_USAGE_BRIDGE_DIR"] {
            directories.append(URL(fileURLWithPath: override))
        }
        if let resources = Bundle.main.resourceURL {
            directories.append(resources.appendingPathComponent("AIUsageIsland_UsageIslandApp.bundle/Bridge"))
            directories.append(resources.appendingPathComponent("Bridge"))
        }
        if let installed = directories.first(where: {
            FileManager.default.fileExists(atPath: $0.appendingPathComponent("usage_bridge.py").path)
        }) {
            script = installed.appendingPathComponent("usage_bridge.py")
        } else if let bundled = Bundle.module.url(forResource: "Bridge", withExtension: nil) {
            script = bundled.appendingPathComponent("usage_bridge.py")
        } else {
            throw BridgeError.resourcesMissing
        }
    }

    func usage(refresh: Bool) async throws -> UsageSnapshot {
        let data = try await run(mode: "ai", refresh: refresh)
        return try UsageSnapshot.decode(data)
    }

    func resources() async throws -> ResourceSnapshot {
        try JSONDecoder().decode(ResourceSnapshot.self, from: await run(mode: "resources"))
    }

    func run(mode: String, refresh: Bool = false) async throws -> Data {
        var environment = ProcessInfo.processInfo.environment
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        environment["PATH"] = "/opt/homebrew/bin:/opt/homebrew/sbin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
        return try await runner.run(
            executable: python,
            arguments: [script.path, mode] + (refresh ? ["--refresh"] : []),
            environment: environment,
            timeout: mode == "resources" ? 10 : 48
        )
    }
}

enum BridgeError: LocalizedError {
    case pythonMissing, resourcesMissing
    var errorDescription: String? {
        switch self {
        case .pythonMissing: return "Python 3 is unavailable. Install it or set AI_USAGE_PYTHON."
        case .resourcesMissing: return "The bundled metrics adapter is missing. Reinstall AI Usage Island."
        }
    }
}
