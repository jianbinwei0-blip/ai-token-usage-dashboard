import Foundation

public struct MetricField: Codable, Equatable, Identifiable, Sendable {
    public let key: String
    public let caption: String
    public let value: String
    public let tone: String
    public var id: String { key }
}

public struct ResetCreditExpiration: Codable, Equatable, Sendable {
    public let count: Int
    public let expiresAt: Int64?
    public let doesNotExpire: Bool

    public var expirationDate: Date? {
        guard let expiresAt, expiresAt > 0, expiresAt <= 253_402_300_799 else { return nil }
        return Date(timeIntervalSince1970: TimeInterval(expiresAt))
    }

    public func hasExpired(at now: Date) -> Bool {
        expirationDate.map { $0 <= now } ?? false
    }

    public func formattedExpiration(locale: Locale = .current, timeZone: TimeZone = .current) -> String {
        guard let date = expirationDate else {
            return doesNotExpire ? "No expiration" : "Not reported"
        }
        let formatter = DateFormatter()
        formatter.locale = locale
        formatter.timeZone = timeZone
        formatter.setLocalizedDateFormatFromTemplate("MMM d yyyy j:mm z")
        return formatter.string(from: date)
    }
}

public struct ResetCreditInfo: Codable, Equatable, Sendable {
    public let availableCount: Int?
    public let expirations: [ResetCreditExpiration]?
    public let detailsComplete: Bool

    public func hasExpiredEntries(at now: Date) -> Bool {
        expirations?.contains { $0.hasExpired(at: now) } ?? false
    }

    public var completenessNote: String? {
        if detailsComplete { return nil }
        if availableCount == 0 && (expirations?.isEmpty ?? true) { return nil }
        guard let expirations, !expirations.isEmpty else {
            return "Expiration dates are unavailable."
        }
        // Do not derive availability from this list: Codex can cap detail rows.
        let detailedCount = expirations.reduce(0) { total, entry in
            let (sum, overflow) = total.addingReportingOverflow(entry.count)
            return overflow ? Int.max : sum
        }
        if let availableCount, detailedCount < availableCount {
            return "Expiration details for \(detailedCount) of \(availableCount) resets were reported."
        }
        return "Reset details are incomplete."
    }
}

public struct UsageSnapshot: Codable, Equatable, Sendable {
    public let schemaVersion: Int
    public let generatedAt: String?
    public let readAt: String
    public let health: String
    public let subscriptionState: String
    public let subscriptionFetchedAt: String?
    public let resetCredits: ResetCreditInfo?
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

/// All displays use notch-style compact and expanded states. Physical hardware
/// affects the cutout geometry, not the presentation policy.
public enum IslandPresentation: Equatable, Sendable {
    case hidden, compact, expanded

    public static func resolve(visible: Bool, details: Bool) -> Self {
        if !visible { return .hidden }
        return details ? .expanded : .compact
    }
}
