// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "DynamicNotchKit",
    platforms: [.macOS(.v14)],
    products: [.library(name: "DynamicNotchKit", targets: ["DynamicNotchKit"])],
    targets: [.target(name: "DynamicNotchKit", path: "Sources")]
)
