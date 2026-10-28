import AppKit
import Combine
import DynamicNotchKit
import SwiftUI
import UsageIslandCore

@MainActor
final class IslandController: NSObject {
    let model = MetricsModel()
    private var island: DynamicNotch<IslandRootView, NotchSummaryView, NotchSummaryView>!
    private var statusItem: NSStatusItem!
    private var shortcut: GlobalShortcut?
    private var shortcutRegistered = false
    private var observations: [(NotificationCenter, NSObjectProtocol)] = []
    private var lockObservations: [NSObjectProtocol] = []
    private var modelObservation: AnyCancellable?
    private var escapeMonitor: Any?
    private var applyingPresentation = false
    private var presentationRequested = false
    private var repositionRequested = false
    private var focusRequested = false
    private var suspended = false
    private var systemSleeping = false
    private var privacyTask: Task<Void, Never>?
    private var previousApplication: NSRunningApplication?
    private var selectedDisplayID = UserDefaults.standard.integer(forKey: "displayID")

    private var selectedScreen: NSScreen? {
        NSScreen.screens.first(where: { Self.displayID($0) == selectedDisplayID }) ?? NSScreen.screens.first
    }

    private static func displayID(_ screen: NSScreen) -> Int {
        (screen.deviceDescription[NSDeviceDescriptionKey("NSScreenNumber")] as? NSNumber)?.intValue ?? 0
    }

    override init() {
        super.init()
        island = DynamicNotch(
            hoverBehavior: [],
            style: .auto,
            expanded: { [model] in IslandRootView(model: model) },
            compactLeading: { [model] in NotchSummaryView(model: model, leading: true) },
            compactTrailing: { [model] in NotchSummaryView(model: model, leading: false) }
        )
        model.toggleDetails = { [weak self] in self?.toggleDetails() }
        model.collapse = { [weak self] in self?.collapse() }
        model.openDashboard = { [weak self] in self?.openDashboard() }
        model.openMenu = { [weak self] in self?.showMenu() }
    }

    private var sessionRequiresSuspension: Bool {
        let session = CGSessionCopyCurrentDictionary() as? [String: Any] ?? [:]
        return SessionPrivacy.shouldSuspend(
            screenLocked: session["CGSSessionScreenIsLocked"] as? Bool,
            onConsole: session[kCGSessionOnConsoleKey as String] as? Bool,
            loginWindowIsFrontmost: NSWorkspace.shared.frontmostApplication?.bundleIdentifier == "com.apple.loginwindow"
        )
    }

    func start() {
        suspended = sessionRequiresSuspension
        // Lock notifications are not replayed to an app launched after locking.
        // Poll as a fallback for startup, unlock races and fast user switching.
        privacyTask = Task { [weak self] in
            while !Task.isCancelled {
                do { try await Task.sleep(for: .seconds(2)) } catch { break }
                guard let self else { break }
                let shouldSuspend = self.systemSleeping || self.sessionRequiresSuspension
                if shouldSuspend != self.suspended {
                    if shouldSuspend {
                        self.suspended = true
                        self.focusRequested = false
                        self.requestPresentation()
                    } else {
                        self.resume()
                    }
                }
            }
        }
        statusItem = NSStatusBar.system.statusItem(withLength: NSStatusItem.squareLength)
        if let button = statusItem.button {
            button.image = NSImage(systemSymbolName: "chart.bar.xaxis", accessibilityDescription: "AI Usage Island")
            button.setAccessibilityLabel("AI Usage Island")
            button.target = self
            button.action = #selector(statusClicked)
            button.sendAction(on: [.leftMouseUp, .rightMouseUp])
        }
        shortcut = GlobalShortcut { [weak self] in self?.toggleDetails() }
        shortcutRegistered = shortcut?.register() ?? false

        observe(NotificationCenter.default, NSApplication.didChangeScreenParametersNotification) { [weak self] in
            self?.repositionRequested = true
            self?.requestPresentation()
        }
        observe(NSWorkspace.shared.notificationCenter, NSWorkspace.willSleepNotification) { [weak self] in
            self?.systemSleeping = true
            self?.suspended = true
            self?.focusRequested = false
            self?.requestPresentation()
        }
        observe(NSWorkspace.shared.notificationCenter, NSWorkspace.didWakeNotification) { [weak self] in
            self?.systemSleeping = false
            self?.resume()
        }
        let distributed = DistributedNotificationCenter.default()
        for (name, locked) in [("com.apple.screenIsLocked", true), ("com.apple.screenIsUnlocked", false)] {
            lockObservations.append(distributed.addObserver(
                forName: NSNotification.Name(name), object: nil, queue: .main
            ) { [weak self] _ in
                MainActor.assumeIsolated {
                    guard let self else { return }
                    if locked {
                        self.suspended = true
                        self.focusRequested = false
                        self.requestPresentation()
                    } else {
                        self.resume()
                    }
                }
            })
        }
        escapeMonitor = NSEvent.addLocalMonitorForEvents(matching: .keyDown) { [weak self] event in
            let consumed = MainActor.assumeIsolated {
                if event.keyCode == 53, self?.model.isExpanded == true {
                    self?.collapse()
                    return true
                }
                return false
            }
            return consumed ? nil : event
        }
        modelObservation = model.objectWillChange.sink { [weak self] _ in
            DispatchQueue.main.async { self?.updateStatus() }
        }
        model.start()
        requestPresentation()
        updateStatus()
    }

    private func observe(_ center: NotificationCenter, _ name: Notification.Name,
                         action: @escaping @MainActor () -> Void) {
        let token = center.addObserver(forName: name, object: nil, queue: .main) { _ in
            MainActor.assumeIsolated { action() }
        }
        observations.append((center, token))
    }

    private func resume() {
        suspended = systemSleeping || sessionRequiresSuspension
        if suspended {
            requestPresentation()
            return
        }
        repositionRequested = true
        requestPresentation()
        Task {
            await model.refresh()
            await model.refreshResources()
        }
    }

    @objc private func statusClicked() {
        if NSApp.currentEvent?.type == .rightMouseUp { showMenu() }
        else { toggleDetails() }
    }

    @objc func toggleDetails() {
        guard !sessionRequiresSuspension else { return }
        let shouldExpand = !model.isExpanded || !model.isVisible
        if shouldExpand {
            let foreground = NSWorkspace.shared.frontmostApplication
            if foreground?.processIdentifier != ProcessInfo.processInfo.processIdentifier {
                previousApplication = foreground
            }
        }
        model.isVisible = true
        model.isExpanded = shouldExpand
        focusRequested = shouldExpand
        requestPresentation()
        if !shouldExpand { restorePreviousApplication() }
    }

    @objc func collapse() {
        model.isExpanded = false
        focusRequested = false
        requestPresentation()
        restorePreviousApplication()
    }

    @objc private func toggleVisibility() {
        model.isVisible.toggle()
        if !model.isVisible {
            model.isExpanded = false
            focusRequested = false
            restorePreviousApplication()
        }
        requestPresentation()
    }

    private func restorePreviousApplication() {
        guard NSWorkspace.shared.frontmostApplication?.processIdentifier ==
                ProcessInfo.processInfo.processIdentifier else { return }
        island.windowController?.window?.resignKey()
        previousApplication?.activate(options: [])
        previousApplication = nil
    }

    private func requestPresentation() {
        presentationRequested = true
        guard !applyingPresentation else { return }
        applyingPresentation = true
        Task {
            while presentationRequested {
                presentationRequested = false
                await applyPresentation()
            }
            applyingPresentation = false
        }
    }

    private func applyPresentation() async {
        guard let screen = selectedScreen else { return }
        if sessionRequiresSuspension {
            suspended = true
            focusRequested = false
        }
        model.hasHardwareNotch = screen.safeAreaInsets.top > 0 &&
            screen.auxiliaryTopLeftArea != nil && screen.auxiliaryTopRightArea != nil
        let reduceMotion = NSWorkspace.shared.accessibilityDisplayShouldReduceMotion
        let animation = Animation.easeOut(duration: reduceMotion ? 0 : 0.18)
        island.transitionConfiguration = .init(
            openingAnimation: animation, closingAnimation: animation,
            conversionAnimation: animation, skipIntermediateHides: true
        )
        if repositionRequested {
            repositionRequested = false
            await island.hide()
        }
        configureWindow(on: screen)
        switch IslandPresentation.resolve(visible: model.isVisible && !suspended,
                                          details: model.isExpanded,
                                          hardwareNotch: model.hasHardwareNotch) {
        case .hidden: await island.hide()
        case .notchCompact: await island.compact(on: screen)
        case .expanded: await island.expand(on: screen)
        }
        configureWindow(on: screen)
        if focusRequested, model.isVisible, model.isExpanded, !suspended {
            focusRequested = false
            NSApp.activate(ignoringOtherApps: true)
            island.windowController?.window?.makeKeyAndOrderFront(nil)
        }
        writeDiagnostics()
    }

    private func configureWindow(on screen: NSScreen) {
        guard let window = island.windowController?.window else { return }
        window.title = "AI Usage Island"
        window.setAccessibilityLabel("AI Usage Island")
        // The library defaults to screenSaver level. A usage utility should not
        // sit above the lock screen or system security UI.
        window.level = .statusBar
        window.collectionBehavior = [.canJoinAllSpaces, .fullScreenAuxiliary, .stationary, .ignoresCycle]
        window.hidesOnDeactivate = false
        window.appearance = NSAppearance(named: .darkAqua)
        window.isMovable = false
        let width: CGFloat = model.isExpanded ? 550 : (model.hasHardwareNotch ? 600 : 440)
        let height: CGFloat = model.isExpanded ? 760 : (model.hasHardwareNotch ? 100 : 160)
        let size = NSSize(width: min(width, screen.frame.width), height: min(height, screen.frame.height))
        window.setFrame(NSRect(x: screen.frame.midX - size.width / 2,
                               y: screen.frame.maxY - size.height,
                               width: size.width, height: size.height), display: true)
    }

    private func updateStatus() {
        statusItem?.button?.toolTip = "AI usage · Today \(model.today) · Weekly \(model.weekly) left\n\(model.status)\nClick: details · Right-click: options"
        writeDiagnostics()
    }

    @objc private func refreshMetrics() {
        Task {
            await model.refresh()
            await model.refreshResources()
        }
    }

    @objc func openDashboard() {
        guard let url = URL(string: "http://127.0.0.1:8765") else { return }
        NSWorkspace.shared.open(url)
    }

    @objc func showMenu() {
        guard let button = statusItem.button else { return }
        let menu = NSMenu()
        func item(_ title: String, _ action: Selector, key: String = "") {
            let item = NSMenuItem(title: title, action: action, keyEquivalent: key)
            item.target = self
            menu.addItem(item)
        }
        item(model.isExpanded ? "Collapse details" : "Show details", #selector(toggleDetails))
        let shortcutInfo = NSMenuItem(title: shortcutRegistered ? "Shortcut: \(GlobalShortcut.displayName)" :
                                        "Shortcut unavailable; use the menu-bar button",
                                     action: nil, keyEquivalent: "")
        shortcutInfo.isEnabled = false
        menu.addItem(shortcutInfo)
        item(model.isVisible ? "Hide island" : "Show island", #selector(toggleVisibility))
        let displayMenu = NSMenu(title: "Display")
        for (name, id) in [("Main display (automatic)", 0)] + NSScreen.screens.map({ ($0.localizedName, Self.displayID($0)) }) {
            let choice = NSMenuItem(title: name, action: #selector(chooseDisplay(_:)), keyEquivalent: "")
            choice.target = self
            choice.tag = id
            choice.state = selectedDisplayID == id ? .on : .off
            displayMenu.addItem(choice)
        }
        let displayItem = NSMenuItem(title: "Display", action: nil, keyEquivalent: "")
        displayItem.submenu = displayMenu
        menu.addItem(displayItem)
        menu.addItem(.separator())
        item("Refresh metrics", #selector(refreshMetrics))
        item("Open usage dashboard", #selector(openDashboard))
        menu.addItem(.separator())
        item("Quit AI Usage Island", #selector(quit), key: "q")
        menu.popUp(positioning: nil, at: NSPoint(x: 0, y: button.bounds.minY - 4), in: button)
    }

    @objc private func chooseDisplay(_ sender: NSMenuItem) {
        selectedDisplayID = sender.tag
        UserDefaults.standard.set(selectedDisplayID, forKey: "displayID")
        repositionRequested = true
        requestPresentation()
    }

    private func writeDiagnostics() {
        let directory = FileManager.default.homeDirectoryForCurrentUser
            .appendingPathComponent("Library/Caches/AIUsageIsland")
        let diagnostic: [String: Any] = [
            "pid": ProcessInfo.processInfo.processIdentifier,
            "visible": model.isVisible && !suspended,
            "suspended": suspended,
            "window_visible": island.windowController?.window?.isVisible ?? false,
            "expanded": model.isExpanded,
            "hardware_notch": model.hasHardwareNotch,
            "shortcut_registered": shortcutRegistered,
            "shortcut": GlobalShortcut.displayName,
            "shortcut_key_code": Int(GlobalShortcut.keyCode),
            "shortcut_modifiers": Int(GlobalShortcut.modifiers),
            "health": model.health,
            "status": model.status,
            "generated_at": model.snapshot?.generatedAt ?? "",
            "screen": selectedScreen?.localizedName ?? "",
            "updated_at": ISO8601DateFormatter().string(from: Date())
        ]
        do {
            try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true,
                                                    attributes: [.posixPermissions: 0o700])
            let data = try JSONSerialization.data(withJSONObject: diagnostic, options: [.sortedKeys, .prettyPrinted])
            try data.write(to: directory.appendingPathComponent("presentation.json"), options: .atomic)
        } catch {
            // Diagnostics must not interrupt the UI or expose collector contents.
        }
    }

    /// Opt-in development smoke check. Captures this app's own views, not the
    /// desktop, so no Screen Recording or Accessibility permission is required.
    func runSmokeTest(in directory: URL) async {
        var checks: [String] = []
        do {
            try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
            for _ in 0..<100 where model.isLoading {
                try await Task.sleep(for: .milliseconds(100))
            }
            try await settle()
            try verify(model.snapshot != nil, "bundled adapter response")
            checks.append("bundled adapter decoded the local cache")
            try captureExpandedPreview(to: directory.appendingPathComponent("expanded-preview.png"))
            checks.append("expanded details rendered offscreen without changing window visibility")
            if sessionRequiresSuspension {
                try verify(suspended && island.windowController == nil, "locked-session startup hides the window")
                checks.append("launch while locked keeps the island hidden")
                writeSmokeReport(directory: directory, checks: checks, error: nil,
                                 skipped: ["Interactive presentation, focus and shortcut checks require an unlocked session."])
                return
            }
            try verify(model.isVisible && !model.isExpanded, "initial compact")
            try capture(to: directory.appendingPathComponent("compact.png"))
            checks.append("compact visible on primary display")

            toggleDetails()
            try await settle()
            try verify(model.isExpanded && island.windowController?.window?.isKeyWindow == true, "expand and focus")
            try capture(to: directory.appendingPathComponent("expanded.png"))
            checks.append("expanded detail view and keyboard focus")

            if let window = island.windowController?.window,
               let escape = NSEvent.keyEvent(with: .keyDown, location: .zero, modifierFlags: [],
                                              timestamp: ProcessInfo.processInfo.systemUptime,
                                              windowNumber: window.windowNumber, context: nil,
                                              characters: "\u{1b}", charactersIgnoringModifiers: "\u{1b}",
                                              isARepeat: false, keyCode: 53) {
                NSApp.postEvent(escape, atStart: false)
            }
            try await Task.sleep(for: .milliseconds(200))
            try await settle()
            try verify(!model.isExpanded, "Escape collapse")
            checks.append("Escape collapses through the native event loop")

            toggleVisibility()
            try await settle()
            try verify(!model.isVisible && island.windowController == nil, "hide")
            toggleVisibility()
            try await settle()
            try verify(model.isVisible && island.windowController?.window?.isVisible == true, "show")
            checks.append("hide and show recreate the panel")

            let originalDisplay = selectedDisplayID
            if let notchedScreen = NSScreen.screens.first(where: { $0.safeAreaInsets.top > 0 }) {
                selectedDisplayID = Self.displayID(notchedScreen)
                repositionRequested = true
                requestPresentation()
                try await settle()
                try verify(model.hasHardwareNotch && !model.isExpanded, "native notch compact")
                try capture(to: directory.appendingPathComponent("hardware-notch-compact.png"))
                toggleDetails()
                try await settle()
                try capture(to: directory.appendingPathComponent("hardware-notch-expanded.png"))
                checks.append("physical notch compact and expanded layouts")
                collapse()
                try await settle()
                selectedDisplayID = originalDisplay
                repositionRequested = true
                requestPresentation()
                try await settle()
                checks.append("return to original display")
            }
            try verify(shortcutRegistered, "global shortcut registration")
            checks.append("\(GlobalShortcut.displayName) registered without Accessibility permission")
            writeSmokeReport(directory: directory, checks: checks, error: nil)
        } catch {
            writeSmokeReport(directory: directory, checks: checks, error: error.localizedDescription)
        }
    }

    private func settle() async throws {
        for _ in 0..<100 {
            try await Task.sleep(for: .milliseconds(50))
            if !applyingPresentation && !presentationRequested {
                try await Task.sleep(for: .milliseconds(250))
                return
            }
        }
        throw NSError(domain: "UsageIslandSmoke", code: 1,
                      userInfo: [NSLocalizedDescriptionKey: "Presentation did not settle"])
    }

    private func verify(_ condition: Bool, _ name: String) throws {
        if !condition {
            throw NSError(domain: "UsageIslandSmoke", code: 2,
                          userInfo: [NSLocalizedDescriptionKey: "Failed: " + name])
        }
    }

    private func captureExpandedPreview(to path: URL) throws {
        // Render only our own SwiftUI view. This never creates an on-screen
        // window, changes focus or weakens the lock-screen suspension policy.
        let renderer = ImageRenderer(content:
            ExpandedUsageView(model: model)
                .foregroundStyle(IslandPalette.text)
                .padding(15)
                .background(IslandPalette.surface)
                .environment(\.colorScheme, .dark)
                .fixedSize()
        )
        renderer.scale = 2
        guard let image = renderer.cgImage,
              let png = NSBitmapImageRep(cgImage: image).representation(using: .png, properties: [:]) else {
            throw NSError(domain: "UsageIslandSmoke", code: 5,
                          userInfo: [NSLocalizedDescriptionKey: "Cannot render expanded preview"])
        }
        try png.write(to: path)
    }

    private func capture(to path: URL) throws {
        guard let view = island.windowController?.window?.contentView,
              let bitmap = view.bitmapImageRepForCachingDisplay(in: view.bounds) else {
            throw NSError(domain: "UsageIslandSmoke", code: 3,
                          userInfo: [NSLocalizedDescriptionKey: "Cannot capture own content view"])
        }
        view.cacheDisplay(in: view.bounds, to: bitmap)
        guard let png = bitmap.representation(using: .png, properties: [:]) else {
            throw NSError(domain: "UsageIslandSmoke", code: 4,
                          userInfo: [NSLocalizedDescriptionKey: "Cannot encode view capture"])
        }
        try png.write(to: path)
    }

    private func writeSmokeReport(directory: URL, checks: [String], error: String?, skipped: [String] = []) {
        let report: [String: Any] = ["passed": error == nil, "checks": checks, "error": error ?? "", "skipped": skipped]
        if let data = try? JSONSerialization.data(withJSONObject: report, options: [.prettyPrinted, .sortedKeys]) {
            try? data.write(to: directory.appendingPathComponent("smoke.json"), options: .atomic)
        }
    }

    @objc private func quit() { NSApp.terminate(nil) }

    func stop() {
        privacyTask?.cancel()
        model.stop()
        shortcut?.unregister()
        if let escapeMonitor { NSEvent.removeMonitor(escapeMonitor) }
        for (center, token) in observations { center.removeObserver(token) }
        for token in lockObservations { DistributedNotificationCenter.default().removeObserver(token) }
        modelObservation?.cancel()
        if let statusItem { NSStatusBar.system.removeStatusItem(statusItem) }
    }
}
