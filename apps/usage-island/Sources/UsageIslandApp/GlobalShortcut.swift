import AppKit
import Carbon

@MainActor
final class GlobalShortcut {
    static let displayName = "Command–Control–U"
    static let keyCode = UInt32(kVK_ANSI_U)
    static let modifiers = UInt32(cmdKey | controlKey)

    private var handler: EventHandlerRef?
    private var hotKey: EventHotKeyRef?
    let action: () -> Void

    init(action: @escaping () -> Void) { self.action = action }

    func register() -> Bool {
        var event = EventTypeSpec(eventClass: OSType(kEventClassKeyboard),
                                  eventKind: UInt32(kEventHotKeyPressed))
        let context = Unmanaged.passUnretained(self).toOpaque()
        let handlerStatus = InstallEventHandler(
            GetApplicationEventTarget(),
            { _, _, context in
                guard let context else { return OSStatus(eventNotHandledErr) }
                let shortcut = Unmanaged<GlobalShortcut>.fromOpaque(context).takeUnretainedValue()
                MainActor.assumeIsolated { shortcut.action() }
                return noErr
            },
            1, &event, context, &handler
        )
        guard handlerStatus == noErr else { return false }
        let id = EventHotKeyID(signature: 0x41495549, id: 1)
        let status = RegisterEventHotKey(
            Self.keyCode, Self.modifiers, id,
            GetApplicationEventTarget(), 0, &hotKey
        )
        if status != noErr {
            if let handler { RemoveEventHandler(handler) }
            handler = nil
        }
        return status == noErr
    }

    func unregister() {
        if let hotKey { UnregisterEventHotKey(hotKey) }
        if let handler { RemoveEventHandler(handler) }
        hotKey = nil
        handler = nil
    }
}
