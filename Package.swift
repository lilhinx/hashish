// swift-tools-version:5.2
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "Hashish",
    platforms: [
        .iOS( .v13 ),
        .macOS( .v10_15 )
    ],
    products: [
        // Products define the executables and libraries produced by a package, and make them visible to other packages.
        .library(
            name: "Hashish",
            targets: ["Hashish"]),
    ],
    dependencies: [
        // Dependencies declare other packages that this package depends on.
        .package( name:"SwiftProtobuf", url:"https://github.com/apple/swift-protobuf", from: "1.11.0" ),
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages which this package depends on.
        .target(
            name: "Hashish",
            dependencies: ["SwiftProtobuf"] ),
        .testTarget(
            name: "HashishTests",
            dependencies: ["Hashish"]),
    ]
)
