plugins {
    id("org.springframework.boot")
    id("io.spring.dependency-management")
    id("com.google.protobuf") version "0.9.4"
}

dependencies {
    implementation(project(":kunefe-proto"))

    // gRPC
    implementation("io.grpc:grpc-netty-shaded:1.64.0")
    implementation("io.grpc:grpc-protobuf:1.64.0")
    implementation("io.grpc:grpc-stub:1.64.0")
    implementation("net.devh:grpc-server-spring-boot-starter:3.1.0.RELEASE")

    // Spring Boot
    implementation("org.springframework.boot:spring-boot-starter")

    // RocksDB
    implementation("org.rocksdb:rocksdbjni:9.2.1")
}