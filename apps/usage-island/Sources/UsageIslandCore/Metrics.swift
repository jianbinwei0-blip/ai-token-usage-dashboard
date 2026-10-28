import Foundation

public struct MetricField: Codable, Equatable, Identifiable, Sendable {
    public let key: String
    public let caption: String
    public let value: String
    public let tone: String
    public var id: String { key }
}

public struct UsageSnapshot: Codable, Equatable, Sendable {
    public let schemaVersion: Int
    public let generatedAt: String?
    public let readAt: String
    public let health: String
    public let subscriptionState: String
    public let subscriptionFetchedAt: String?
    public let pricingComplete: Bool
    public let scope: String
    public let providers: [String]
    public let text: String
    public let fields: [MetricField]
    public let message: String?

    public func field(_ key: String) -> MetricField? { fields.first { $0.key == key } }

    public static func decode(_ data: Data) throws -> UsageSnapshot {
        let decoder = JSONDecoder()
        decoder.keyDecodingStrategy = .convertFromSnakeCase
        let snapshot = try decoder.decode(Self.self, from: data)
        guard snapshot.schemaVersion == 1,
              Set(snapshot.fields.map(\.key)).count == snapshot.fields.count else {
            throw SnapshotError.unsupportedSchema
        }
        return snapshot
    }

    public func effectiveHealth(at now: Date) -> String {
        guard let date = Self.date(generatedAt) else {
            return health == "unavailable" ? "unavailable" : "error"
        }
        if ["error", "unavailable", "stale"].contains(health) { return health }
        // A previously partial snapshot can also age out while the Mac sleeps.
        if now.timeIntervalSince(date) > 330 { return "stale" }
        return ["ok", "partial"].contains(health) ? health : "error"
    }

    public func quotaIsStale(at now: Date) -> Bool {
        if subscriptionState == "stale" { return true }
        guard subscriptionState == "ok", let date = Self.date(subscriptionFetchedAt) else { return false }
        return now.timeIntervalSince(date) > 900
    }

    public var compactWeekly: String? {
        field("weekly")?.value.replacingOccurrences(of: " left", with: "")
    }

    public static func date(_ value: String?) -> Date? {
        guard let value else { return nil }
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        if let date = formatter.date(from: value) { return date }
        formatter.formatOptions = [.withInternetDateTime]
        return formatter.date(from: value)
    }
}

/// Fail closed for known lock/login-window or inactive-console states.
public enum SessionPrivacy {
    public static func shouldSuspend(screenLocked: Bool?, onConsole: Bool?, loginWindowIsFrontmost: Bool) -> Bool {
        screenLocked == true || onConsole == false || loginWindowIsFrontmost
    }
}

public enum SnapshotError: Error {
    case unsupportedSchema
}

public struct ResourceSnapshot: Codable, Equatable, Sendable {
    public let ram: String
    public let disk: String
    public let available: Bool

    public init(ram: String = "n/a", disk: String = "n/a", available: Bool = false) {
        self.ram = ram
        self.disk = disk
        self.available = available
    }
}

/// DynamicNotchKit hides its native compact state on non-notched screens.
/// Keep its floating panel expanded and swap our content instead.
public enum IslandPresentation: Equatable, Sendable {
    case hidden, notchCompact, expanded

    public static func resolve(visible: Bool, details: Bool, hardwareNotch: Bool) -> Self {
        if !visible { return .hidden }
        return hardwareNotch && !details ? .notchCompact : .expanded
    }
}
