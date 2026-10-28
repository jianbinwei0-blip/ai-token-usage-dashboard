// A standalone test executable keeps checks runnable with Command Line Tools,
// which ship neither XCTest nor the Testing module on this machine.
import Foundation
import UsageIslandCore

private let fixture = """
{
  "schema_version": 1, "generated_at": "2026-10-27T20:00:00Z",
  "read_at": "2026-10-27T20:00:05.123456+00:00", "health": "ok",
  "subscription_state": "ok", "subscription_fetched_at": "2026-10-27T20:00:00Z",
  "pricing_complete": true, "scope": "combined", "providers": ["codex", "claude", "pi", "dsh"],
  "text": "Today 8.6M · Weekly 99% left",
  "fields": [
    {"key": "today", "caption": "Today", "value": "8.6M", "tone": "text"},
    {"key": "weekly", "caption": "Weekly", "value": "99% left", "tone": "positive"}
  ], "message": null
}
"""

private struct Failure: Error, CustomStringConvertible {
    let description: String
}
private func check(_ condition: @autoclosure () -> Bool, _ message: String) throws {
    if !condition() { throw Failure(description: message) }
}
private func snapshot(_ text: String = fixture) throws -> UsageSnapshot {
    try UsageSnapshot.decode(Data(text.utf8))
}

private func snapshotWithResets(_ resets: [String: Any]) throws -> UsageSnapshot {
    var object = try JSONSerialization.jsonObject(with: Data(fixture.utf8)) as! [String: Any]
    object["reset_credits"] = resets
    return try UsageSnapshot.decode(JSONSerialization.data(withJSONObject: object))
}

@main
enum CoreChecks {
    static func main() async {
        let checks: [(String, () async throws -> Void)] = [
            ("bridge contract and exact display values", {
                let value = try snapshot()
                try check(value.field("today")?.value == "8.6M", "Today changed")
                try check(value.compactWeekly == "99%", "Quota changed")
                try check(value.providers == ["codex", "claude", "pi", "dsh"], "Scope changed")
                try check(value.field("cost") == nil, "Missing cost invented")
            }),
            ("reset metadata is optional for legacy caches", {
                let legacy = try snapshot()
                try check(legacy.resetCredits == nil, "Legacy bridge compatibility")
                let value = try snapshotWithResets([
                    "available_count": 5, "details_complete": false,
                    "expirations": [["count": 2, "expires_at": 1_900_000_000, "does_not_expire": false]]
                ])
                try check(value.resetCredits?.availableCount == 5, "Do not derive total from capped rows")
                try check(value.resetCredits?.completenessNote?.contains("2 of 5") == true, "Capped details need a label")
                try check(value.field("today")?.value == "8.6M", "Reset details changed token totals")
            }),
            ("zero resets, unavailable dates and no expiration are distinct", {
                let zero = try snapshotWithResets(["available_count": 0, "expirations": [], "details_complete": true])
                try check(zero.resetCredits?.availableCount == 0, "Known zero is not unknown")
                try check(zero.resetCredits?.completenessNote == nil, "No dates needed for zero resets")
                let value = try snapshotWithResets([
                    "available_count": 2, "details_complete": true,
                    "expirations": [
                        ["count": 1, "expires_at": NSNull(), "does_not_expire": true],
                        ["count": 1, "expires_at": NSNull(), "does_not_expire": false],
                    ]
                ])
                let entries = value.resetCredits!.expirations!
                try check(entries[0].formattedExpiration() == "No expiration", "Explicit null means no expiration")
                try check(entries[1].formattedExpiration() == "Not reported", "Missing dates must not become never")
                let missing = try snapshotWithResets([
                    "available_count": 2, "expirations": NSNull(), "details_complete": false
                ])
                try check(missing.resetCredits?.completenessNote == "Expiration dates are unavailable.", "Missing detail warning")
            }),
            ("reset expiry uses local time and the date's daylight-saving offset", {
                let summer = Int64(UsageSnapshot.date("2030-06-01T12:00:00Z")!.timeIntervalSince1970)
                let winter = Int64(UsageSnapshot.date("2030-12-01T12:00:00Z")!.timeIntervalSince1970)
                let value = try snapshotWithResets([
                    "available_count": 2, "details_complete": true,
                    "expirations": [
                        ["count": 1, "expires_at": summer, "does_not_expire": false],
                        ["count": 1, "expires_at": winter, "does_not_expire": false],
                    ]
                ])
                let entries = value.resetCredits!.expirations!
                let locale = Locale(identifier: "en_US")
                let zone = TimeZone(identifier: "America/Los_Angeles")!
                let first = entries[0].formattedExpiration(locale: locale, timeZone: zone)
                let last = entries[1].formattedExpiration(locale: locale, timeZone: zone)
                try check(first.contains("2030") && first.contains("5:00") && first.contains("PDT"), first)
                try check(last.contains("2030") && last.contains("4:00") && last.contains("PST"), last)
                let boundary = Date(timeIntervalSince1970: TimeInterval(summer))
                try check(!entries[0].hasExpired(at: boundary.addingTimeInterval(-1)), "Before expiry")
                try check(entries[0].hasExpired(at: boundary), "At expiry")
                try check(value.resetCredits!.hasExpiredEntries(at: boundary), "Expired cached entries need a warning")
                try check(value.resetCredits?.availableCount == 2, "Never silently rewrite the last reported count")
            }),
            ("reject unsupported schema and duplicate fields", {
                for text in [
                    fixture.replacingOccurrences(of: "\"schema_version\": 1", with: "\"schema_version\": 2"),
                    fixture.replacingOccurrences(of: "\"key\": \"weekly\"", with: "\"key\": \"today\""),
                    "{}"
                ] {
                    var rejected = false
                    do { _ = try snapshot(text) } catch { rejected = true }
                    try check(rejected, "Invalid bridge contract accepted")
                }
            }),
            ("usage and quota age independently", {
                let value = try snapshot()
                let generated = UsageSnapshot.date(value.generatedAt)!
                try check(value.effectiveHealth(at: generated.addingTimeInterval(330)) == "ok", "Fresh boundary")
                try check(value.effectiveHealth(at: generated.addingTimeInterval(331)) == "stale", "Stale boundary")
                try check(!value.quotaIsStale(at: generated.addingTimeInterval(900)), "Quota fresh boundary")
                try check(value.quotaIsStale(at: generated.addingTimeInterval(901)), "Quota stale boundary")
            }),
            ("partial and failed data never become fresh", {
                let now = UsageSnapshot.date("2026-10-27T21:00:00Z")!
                let partial = try snapshot(fixture.replacingOccurrences(of: "\"health\": \"ok\"",
                                                                       with: "\"health\": \"partial\""))
                try check(partial.effectiveHealth(at: now) == "stale", "Partial cache failed to age")
                let failed = try snapshot(fixture.replacingOccurrences(of: "\"health\": \"ok\"",
                                                                      with: "\"health\": \"error\""))
                try check(failed.effectiveHealth(at: now) == "error", "Failure concealed")
            }),
            ("missing timestamps and fractional ISO dates", {
                let invalid = try snapshot(fixture.replacingOccurrences(
                    of: "\"generated_at\": \"2026-10-27T20:00:00Z\"", with: "\"generated_at\": null"))
                try check(invalid.effectiveHealth(at: Date()) == "error", "Missing timestamp is fresh")
                try check(UsageSnapshot.date("not-a-date") == nil, "Invalid date accepted")
                try check(UsageSnapshot.date("2026-10-27T20:00:05.123456+00:00") != nil, "Fractional ISO date")
            }),
            ("floating compact stays visible; hardware notch uses native compact", {
                try check(IslandPresentation.resolve(visible: true, details: false, hardwareNotch: false) == .expanded,
                          "Floating compact must not call kit.compact()")
                try check(IslandPresentation.resolve(visible: true, details: true, hardwareNotch: false) == .expanded,
                          "Floating details")
                try check(IslandPresentation.resolve(visible: true, details: false, hardwareNotch: true) == .notchCompact,
                          "Hardware compact")
                try check(IslandPresentation.resolve(visible: true, details: true, hardwareNotch: true) == .expanded,
                          "Hardware expanded")
                try check(IslandPresentation.resolve(visible: false, details: true, hardwareNotch: false) == .hidden,
                          "Explicit hide")
            }),
            ("lock and inactive-session privacy policy", {
                try check(SessionPrivacy.shouldSuspend(screenLocked: true, onConsole: true, loginWindowIsFrontmost: false),
                          "Known lock must hide the island")
                try check(SessionPrivacy.shouldSuspend(screenLocked: false, onConsole: false, loginWindowIsFrontmost: false),
                          "Inactive console must hide the island")
                try check(SessionPrivacy.shouldSuspend(screenLocked: nil, onConsole: nil, loginWindowIsFrontmost: true),
                          "Login-window fallback must hide the island")
                try check(!SessionPrivacy.shouldSuspend(screenLocked: false, onConsole: true, loginWindowIsFrontmost: false),
                          "Unlocked console must be visible")
            }),
            ("resource errors never invent zero usage", {
                let missing = ResourceSnapshot()
                try check(missing.ram == "n/a" && missing.disk == "n/a" && !missing.available, "Resource fallback")
                let decoded = try JSONDecoder().decode(ResourceSnapshot.self, from: Data("""
                {"ram":"72%","disk":"61%","available":true}
                """.utf8))
                try check(decoded.ram == "72%" && decoded.disk == "61%", "Resource values changed")
            }),
            ("subprocess output and exit status", {
                let runner = ProcessRunner()
                let output = try await runner.run(executable: URL(fileURLWithPath: "/bin/echo"),
                                                  arguments: ["metrics"], timeout: 3)
                try check(String(decoding: output, as: UTF8.self) == "metrics\n", "Lost stdout")
                var failed = false
                do {
                    _ = try await runner.run(executable: URL(fileURLWithPath: "/usr/bin/false"),
                                             arguments: [], timeout: 3)
                } catch ProcessFailure.unsuccessful { failed = true }
                try check(failed, "Nonzero exit accepted")
            }),
            ("stuck helper is terminated", {
                let started = Date()
                var timedOut = false
                do {
                    _ = try await ProcessRunner().run(executable: URL(fileURLWithPath: "/bin/sleep"),
                                                      arguments: ["10"], timeout: 0.1)
                } catch ProcessFailure.timedOut { timedOut = true }
                try check(timedOut, "Timeout not reported")
                try check(Date().timeIntervalSince(started) < 5, "Child not terminated")
            }),
            ("oversized helper output is rejected without pipe deadlock", {
                var rejected = false
                do {
                    _ = try await ProcessRunner().run(executable: URL(fileURLWithPath: "/bin/dd"),
                                                      arguments: ["if=/dev/zero", "bs=1024", "count=1025"], timeout: 3)
                } catch ProcessFailure.outputTooLarge { rejected = true }
                try check(rejected, "Oversized response accepted")
            })
        ]
        var failures = 0
        for (name, run) in checks {
            do {
                try await run()
                print("PASS: \(name)")
            } catch {
                failures += 1
                print("FAIL: \(name): \(error)")
            }
        }
        print("\(checks.count - failures)/\(checks.count) native checks passed")
        exit(failures == 0 ? 0 : 1)
    }
}
