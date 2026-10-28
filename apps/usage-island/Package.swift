// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "AIUsageIsland",
    platforms: [.macOS(.v14)],
    products: [.executable(name: "UsageIsland", targets: ["UsageIslandApp"])],
    dependencies: [
        .package(path: "Vendor/DynamicNotchKit")
    ],
    targets: [
        .target(name: "UsageIslandCore"),
        .executableTarget(
            name: "UsageIslandApp",
            dependencies: ["UsageIslandCore", "DynamicNotchKit"],
            resources: [.copy("Resources/Bridge")]
        ),
        .executableTarget(name: "UsageIslandCoreChecks", dependencies: ["UsageIslandCore"],
                          path: "Tests/UsageIslandCoreTests")
    ],
    swiftLanguageModes: [.v5]
)
