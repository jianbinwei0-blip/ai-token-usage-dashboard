import SwiftUI
import UsageIslandCore

enum IslandPalette {
    static let surface = Color(red: 21/255, green: 24/255, blue: 29/255)
    static let text = Color(red: 227/255, green: 231/255, blue: 237/255)
    static let caption = Color(red: 142/255, green: 172/255, blue: 204/255)
    static let positive = Color(red: 164/255, green: 210/255, blue: 183/255)
    static let warning = Color(red: 231/255, green: 189/255, blue: 123/255)
    static let error = Color(red: 243/255, green: 154/255, blue: 150/255)
    static let separator = Color(red: 42/255, green: 48/255, blue: 56/255)
    static let valueFont = Font.custom("JetBrainsMono-Regular", size: 13)

    static func tone(_ tone: String) -> Color {
        switch tone {
        case "positive": return positive
        case "warning": return warning
        case "error": return error
        default: return text
        }
    }
}

struct IslandRootView: View {
    @ObservedObject var model: MetricsModel

    var body: some View {
        Group {
            if model.isExpanded || model.hasHardwareNotch {
                ExpandedUsageView(model: model)
            } else {
                FloatingSummaryView(model: model)
            }
        }
        .foregroundStyle(IslandPalette.text)
        .background {
            if !model.hasHardwareNotch {
                // Fill the kit's 15pt content inset without altering its layout.
                IslandPalette.surface.padding(-15)
            }
        }
        .preferredColorScheme(.dark)
        .onExitCommand(perform: model.collapse)
    }
}

struct FloatingSummaryView: View {
    @ObservedObject var model: MetricsModel

    var body: some View {
        Button(action: model.toggleDetails) {
            HStack(spacing: 14) {
                Image(systemName: model.needsAttention ? "exclamationmark.circle" : "chart.bar.xaxis")
                    .foregroundStyle(model.needsAttention ? IslandPalette.warning : IslandPalette.caption)
                    .font(.system(size: 13))
                compactValue("Today", value: model.today)
                Rectangle().fill(IslandPalette.separator).frame(width: 1, height: 14)
                compactValue("Week left", value: model.weekly,
                             tone: model.quotaIsStale ? "warning" : model.snapshot?.field("weekly")?.tone ?? "text")
                Image(systemName: "chevron.down")
                    .font(.system(size: 10, weight: .semibold))
                    .foregroundStyle(IslandPalette.caption)
            }
            .frame(height: 24)
            .fixedSize(horizontal: true, vertical: false)
            .contentShape(Rectangle())
        }
        .buttonStyle(.plain)
        .accessibilityLabel("Show usage details. Today \(model.today) tokens. Weekly \(model.weekly) remaining. \(model.status).")
        .help("Show usage details (Control–Option–U). " + model.status)
    }

    private func compactValue(_ title: String, value: String, tone: String = "text") -> some View {
        HStack(spacing: 6) {
            Text(title).font(.system(size: 12)).foregroundStyle(IslandPalette.caption)
            Text(value).font(IslandPalette.valueFont).monospacedDigit()
                .foregroundStyle(IslandPalette.tone(tone))
                .fixedSize()
        }
    }
}

struct NotchSummaryView: View {
    @ObservedObject var model: MetricsModel
    let leading: Bool

    var body: some View {
        Button(action: model.toggleDetails) {
            HStack(spacing: 6) {
                if leading && model.needsAttention {
                    Image(systemName: "exclamationmark.circle")
                        .foregroundStyle(IslandPalette.warning)
                }
                Text(leading ? "Today" : "Week left")
                    .foregroundStyle(IslandPalette.caption)
                    .font(.system(size: 11))
                Text(leading ? model.today : model.weekly)
                    .font(IslandPalette.valueFont)
                    .foregroundStyle(leading ? IslandPalette.text :
                        IslandPalette.tone(model.quotaIsStale ? "warning" : model.snapshot?.field("weekly")?.tone ?? "text"))
            }
            .padding(.horizontal, 4)
            .frame(minHeight: 24)
            .contentShape(Rectangle())
        }
        .buttonStyle(.plain)
        .accessibilityLabel(leading ? "Show usage details. Today \(model.today) tokens. \(model.status)." :
                            "Show usage details. Weekly \(model.weekly) remaining. \(model.status).")
        .help("Show usage details (Control–Option–U)")
        .preferredColorScheme(.dark)
    }
}

struct ExpandedUsageView: View {
    @ObservedObject var model: MetricsModel

    private var subscriptionFields: [MetricField] {
        model.snapshot?.fields.filter { ["weekly", "reset", "credits"].contains($0.key) } ?? []
    }
    private var usageFields: [MetricField] {
        model.snapshot?.fields.filter { ["today", "month", "cost"].contains($0.key) } ?? []
    }
    private var extras: [MetricField] {
        model.snapshot?.fields.filter { $0.key.hasPrefix("extra") } ?? []
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            header
            if let message = model.detailMessage {
                Text(message)
                    .font(.system(size: 12))
                    .foregroundStyle(IslandPalette.warning)
                    .fixedSize(horizontal: false, vertical: true)
                    .accessibilityLabel(message)
            }

            if let plan = model.snapshot?.field("plan"), plan.value != "AI" {
                VStack(alignment: .leading, spacing: 10) {
                    HStack {
                        Text(plan.value.replacingOccurrences(of: " stale", with: ""))
                            .font(.system(size: 13, weight: .semibold))
                            .foregroundStyle(IslandPalette.positive)
                        Spacer()
                        if model.quotaIsStale {
                            Text("Quota stale").font(.system(size: 11))
                                .foregroundStyle(IslandPalette.warning)
                        }
                    }
                    ForEach(subscriptionFields) { field in
                        metricRow(field, stale: model.quotaIsStale)
                    }
                    ForEach(extras) { field in
                        metricRow(field)
                    }
                    if let resetCredits = model.snapshot?.resetCredits {
                        ResetCreditsView(info: resetCredits, stale: model.quotaIsStale || model.health == "error", now: model.now)
                    }
                }
                separator
            } else if !extras.isEmpty {
                ForEach(extras) { metricRow($0) }
                separator
            }

            if usageFields.isEmpty {
                VStack(alignment: .leading, spacing: 6) {
                    Text(model.isLoading ? "Reading your existing usage cache…" : "Token totals unavailable")
                        .font(.system(size: 13))
                    Text("No replacement values are estimated.")
                        .font(.system(size: 12)).foregroundStyle(IslandPalette.caption)
                }
            } else {
                VStack(alignment: .leading, spacing: 10) {
                    ForEach(usageFields) { metricRow($0) }
                }
            }

            VStack(alignment: .leading, spacing: 5) {
                Text("Estimated cost is a usage valuation, not an additional subscription charge.")
                if model.snapshot?.pricingComplete == false && !usageFields.isEmpty {
                    Text("* Some usage has no configured price; the estimate is incomplete.")
                        .foregroundStyle(IslandPalette.warning)
                }
            }
            .font(.system(size: 11))
            .foregroundStyle(IslandPalette.caption)
            .fixedSize(horizontal: false, vertical: true)

            separator

            VStack(spacing: 10) {
                HStack {
                    Text("Collected").foregroundStyle(IslandPalette.caption)
                    Spacer()
                    Text(model.lastCollected).monospacedDigit()
                }
                .font(.system(size: 11))
                HStack(spacing: 20) {
                    resource("RAM", value: model.resources.ram)
                    resource("Disk", value: model.resources.disk)
                    Spacer(minLength: 0)
                    Text("Every 5 min").font(.system(size: 11))
                        .foregroundStyle(IslandPalette.caption)
                        .help("AI collection is cached on five-minute boundaries. RAM refreshes every ten seconds.")
                }
                if let refresh = model.snapshot?.field("refresh") {
                    Text("Collection → next scheduled: " + refresh.value)
                        .font(.system(size: 11)).monospacedDigit()
                        .foregroundStyle(IslandPalette.caption)
                        .frame(maxWidth: .infinity, alignment: .leading)
                }
            }

            HStack(spacing: 8) {
                Button(action: { Task { await model.refresh() } }) {
                    Label(model.isRefreshing ? "Refreshing…" : "Refresh", systemImage: "arrow.clockwise")
                }
                .disabled(model.isRefreshing)
                .help("Refresh the view using the existing five-minute collection cache.")
                Button("Dashboard", action: model.openDashboard)
                    .help("Open the existing local usage dashboard.")
                Spacer()
                Button(action: model.openMenu) {
                    Image(systemName: "ellipsis")
                        .frame(width: 20)
                }
                .accessibilityLabel("More options")
                .help("More options")
            }
            .controlSize(.small)
            .buttonStyle(.bordered)
            .font(.system(size: 12))
            .tint(IslandPalette.caption)

            Text(model.sourceDescription)
                .font(.system(size: 10))
                .foregroundStyle(IslandPalette.caption)
                .fixedSize(horizontal: false, vertical: true)
        }
        .frame(width: 400)
        .padding(.horizontal, 6)
        .padding(.vertical, 4)
        .transaction { transaction in
            // Values are status, not a ticker. Do not animate refreshes.
            transaction.animation = nil
        }
    }

    private var header: some View {
        HStack(alignment: .top) {
            VStack(alignment: .leading, spacing: 5) {
                Text("AI usage").font(.system(size: 16, weight: .semibold))
                Label(model.status, systemImage: model.needsAttention ? "exclamationmark.circle" : "circle.fill")
                    .font(.system(size: 11))
                    .foregroundStyle(model.needsAttention ? IslandPalette.warning : IslandPalette.caption)
            }
            Spacer()
            Button(action: model.collapse) {
                Image(systemName: "chevron.up")
                    .font(.system(size: 12, weight: .semibold))
                    .frame(width: 26, height: 26)
                    .contentShape(Rectangle())
            }
            .buttonStyle(.plain)
            .foregroundStyle(IslandPalette.caption)
            .accessibilityLabel("Collapse usage details")
            .help("Collapse (Escape)")
        }
    }

    private var separator: some View {
        Rectangle().fill(IslandPalette.separator).frame(height: 1)
    }

    private func metricRow(_ field: MetricField, stale: Bool = false) -> some View {
        HStack(alignment: .firstTextBaseline, spacing: 16) {
            Text(field.key == "cost" ? "Estimated MTD cost" : field.key == "reset" ? "Weekly reset" : field.caption)
                .font(.system(size: 12))
                .foregroundStyle(IslandPalette.caption)
            Spacer(minLength: 8)
            Text(field.value)
                .font(IslandPalette.valueFont)
                .monospacedDigit()
                .foregroundStyle(IslandPalette.tone(stale ? "warning" : field.tone))
                .multilineTextAlignment(.trailing)
                .fixedSize(horizontal: false, vertical: true)
        }
        .accessibilityElement(children: .combine)
    }

    private func resource(_ label: String, value: String) -> some View {
        HStack(spacing: 6) {
            Text(label).foregroundStyle(IslandPalette.caption).font(.system(size: 11))
            Text(value).font(IslandPalette.valueFont).monospacedDigit()
        }
        .accessibilityElement(children: .combine)
    }
}
