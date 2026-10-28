# Vendored DynamicNotchKit

Source: https://github.com/MrKai77/DynamicNotchKit
Revision: cd0b3e52d537db115ad3a9d89601f20e0bee8d27
License: MIT, preserved in LICENSE and ../../LICENSES/DynamicNotchKit.txt.

All Swift files in Sources are the pinned upstream sources, except:
- Utility/EnvironmentValues+Extensions.swift: replace two @Entry macros with equivalent EnvironmentKey definitions.
- Views/NotchShape.swift: make the Xcode-only #Preview opt-in with ENABLE_DYNAMIC_NOTCH_PREVIEWS.

The local package omits the documentation plugin, DocC catalog/demo video and upstream tests, and sets macOS 14 as the minimum to match the application. No UI algorithms or notch geometry are changed.

These small compatibility changes support Command Line Tools-only builds. The installed CLT has no SwiftUIMacros or PreviewsMacros plugins. Build against the installed macOS 26 SDK, not its default macOS 27 SDK: the latter also changes @State into a macro. scripts/swift.sh selects an installed SDK matching the running macOS major version and supports AI_USAGE_SDK as an explicit override.

To update: review a new upstream revision, replace Sources, reapply the two changes above, update this revision, and run all app checks. Do not patch SwiftPM's transient .build/checkouts tree.
