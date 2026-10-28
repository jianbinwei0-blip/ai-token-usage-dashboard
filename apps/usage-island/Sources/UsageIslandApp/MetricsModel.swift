import AppKit
import Combine
import Foundation
import UsageIslandCore

@MainActor
final class MetricsModel: ObservableObject {
    @Published private(set) var snapshot: UsageSnapshot?
    @Published private(set) var resources = ResourceSnapshot()
    @Published private(set) var isRefreshing = false
    @Published private(set) var isLoading = true
    @Published private(set) var bridgeError: String?
    @Published private(set) var now = Date()
    @Published var isExpanded = false
    @Published var isVisible = true
    @Published var hasHardwareNotch = false

    var toggleDetails: () -> Void = {}
    var collapse: () -> Void = {}
    var openDashboard: () -> Void = {}
    var openMenu: () -> Void = {}

    private var bridge: BridgeService?
    private var usageTask: Task<Void, Never>?
    private var resourcesTask: Task<Void, Never>?
    private var clockTask: Task<Void, Never>?

    var health: String {
        if bridgeError != nil { return "error" }
        return snapshot?.effectiveHealth(at: now) ?? (isLoading ? "loading" : "unavailable")
    }

    var quotaIsStale: Bool { snapshot?.quotaIsStale(at: now) ?? false }
    var today: String { snapshot?.field("today")?.value ?? (isLoading ? "…" : "n/a") }
    var weekly: String { snapshot?.compactWeekly ?? "n/a" }

    var status: String {
        switch health {
        case "loading": return "Loading usage"
        case "unavailable": return "Usage unavailable"
        case "error": return "Collection error"
        case "stale": return "Cached usage is stale"
        case "partial": return "Pricing incomplete"
        default:
            if quotaIsStale { return "Quota is stale" }
            if snapshot?.subscriptionState == "unavailable" { return "Quota unavailable" }
            if snapshot?.field("weekly")?.tone == "error" { return "Weekly quota exhausted" }
            if snapshot?.field("weekly")?.tone == "warning" { return "Weekly quota is low" }
            return "Up to date"
        }
    }

    var needsAttention: Bool {
        !["ok", "loading"].contains(health) || quotaIsStale ||
        snapshot?.subscriptionState == "unavailable" ||
        ["warning", "error"].contains(snapshot?.field("weekly")?.tone ?? "")
    }

    var detailMessage: String? {
        bridgeError ?? snapshot?.message
    }

    var lastCollected: String {
        guard let date = UsageSnapshot.date(snapshot?.generatedAt) else { return "Not yet collected" }
        return date.formatted(date: .abbreviated, time: .shortened)
    }

    var sourceDescription: String {
        guard let snapshot, !snapshot.providers.isEmpty else { return "Existing local dashboard" }
        let names = ["codex": "Codex", "claude": "Claude", "pi": "Pi", "dsh": "DeepSeek Harness"]
        return snapshot.providers.map { names[$0] ?? $0 }.joined(separator: " · ")
    }

    func start() {
        do {
            bridge = try BridgeService()
        } catch {
            bridgeError = error.localizedDescription
            isLoading = false
            return
        }
        usageTask = Task { [weak self] in
            guard let self else { return }
            await readCached()
            while !Task.isCancelled {
                await refresh()
                do { try await Task.sleep(for: .seconds(30)) } catch { break }
            }
        }
        resourcesTask = Task { [weak self] in
            guard let self else { return }
            while !Task.isCancelled {
                await refreshResources()
                do { try await Task.sleep(for: .seconds(10)) } catch { break }
            }
        }
        clockTask = Task { [weak self] in
            while !Task.isCancelled {
                do { try await Task.sleep(for: .seconds(10)) } catch { break }
                self?.now = Date()
            }
        }
    }

    private func readCached() async {
        guard let bridge else { return }
        do {
            snapshot = try await bridge.usage(refresh: false)
            bridgeError = nil
        } catch {
            bridgeError = "Cannot read usage. " + error.localizedDescription
        }
        isLoading = false
        now = Date()
    }

    func refresh() async {
        guard !isRefreshing, let bridge else { return }
        isRefreshing = true
        defer { isRefreshing = false; isLoading = false; now = Date() }
        do {
            snapshot = try await bridge.usage(refresh: true)
            bridgeError = nil
        } catch {
            // Retain the last good snapshot, but never call it fresh.
            bridgeError = "Refresh failed. Cached values may be out of date. " + error.localizedDescription
        }
    }

    func refreshResources() async {
        guard let bridge else { return }
        do {
            resources = try await bridge.resources()
        } catch {
            resources = ResourceSnapshot()
        }
    }

    func stop() {
        usageTask?.cancel()
        resourcesTask?.cancel()
        clockTask?.cancel()
        bridge?.runner.terminateAll()
    }
}
