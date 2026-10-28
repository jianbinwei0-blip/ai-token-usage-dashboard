import SwiftUI
import UsageIslandCore

struct ResetCreditsView: View {
    let info: ResetCreditInfo
    let stale: Bool
    let now: Date

    private var expired: Bool { info.hasExpiredEntries(at: now) }
    private var groups: [ResetCreditExpiration] { info.expirations ?? [] }

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            HStack(alignment: .firstTextBaseline) {
                Text(stale || expired ? "Resets (last reported)" : "Available resets")
                    .font(.system(size: 12))
                    .foregroundStyle(IslandPalette.caption)
                Spacer()
                Text(info.availableCount.map(String.init) ?? "Unavailable")
                    .font(IslandPalette.valueFont)
                    .foregroundStyle(stale || expired ? IslandPalette.warning : IslandPalette.text)
            }
            .accessibilityElement(children: .combine)

            if groups.count > 3 {
                ScrollView(.vertical) { expirationRows }
                    .frame(height: 90)
                    .accessibilityLabel("Reset expiration dates")
            } else {
                expirationRows
            }

            if let note = info.completenessNote {
                Text(note)
                    .font(.system(size: 11))
                    .foregroundStyle(IslandPalette.caption)
                    .fixedSize(horizontal: false, vertical: true)
            }
            if expired {
                Text("A listed reset has expired. Refresh to confirm availability.")
                    .font(.system(size: 11))
                    .foregroundStyle(IslandPalette.warning)
                    .fixedSize(horizontal: false, vertical: true)
            } else if stale {
                Text("Reset information is cached and may be out of date.")
                    .font(.system(size: 11))
                    .foregroundStyle(IslandPalette.warning)
                    .fixedSize(horizontal: false, vertical: true)
            }
        }
        .padding(.top, 3)
        .help("Read-only reset credits. Expiration dates use your Mac’s local time zone. No resets are consumed.")
    }

    private var expirationRows: some View {
        VStack(alignment: .leading, spacing: 6) {
            ForEach(Array(groups.enumerated()), id: \.offset) { _, group in
                HStack(alignment: .firstTextBaseline, spacing: 12) {
                    Text(label(for: group))
                        .foregroundStyle(IslandPalette.caption)
                    Spacer(minLength: 4)
                    Text(group.formattedExpiration())
                        .foregroundStyle(group.hasExpired(at: now) || stale ? IslandPalette.warning : IslandPalette.text)
                        .monospacedDigit()
                        .multilineTextAlignment(.trailing)
                        .fixedSize(horizontal: false, vertical: true)
                }
                .font(.system(size: 11))
                .accessibilityElement(children: .combine)
            }
        }
    }

    private func label(for group: ResetCreditExpiration) -> String {
        let subject = "\(group.count) " + (group.count == 1 ? "reset" : "resets")
        guard group.expirationDate != nil else { return subject }
        if group.hasExpired(at: now) { return subject + " expired" }
        return subject + (group.count == 1 ? " expires" : " expire")
    }
}
