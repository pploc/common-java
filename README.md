# common-java

Shared Java foundations for Gym microservices, built with Java 26 and Spring Boot 4.

## Status

Published stable line is `com.gym:common-java:2.0.1` (tag `v2.0.1`). `develop` continues as `2.0.0-SNAPSHOT` for unreleased work; consume only published immutable versions in services.

## Kafka transport

- Publishes concrete generated Protobuf messages using Confluent Schema Registry framing and `TopicNameStrategy` (`<topic>-value`).
- Runtime clients are lookup-only: `auto.register.schemas=false`.
- Ingress keeps raw key, framed value, and ordered headers until processing finishes.
- A handler receives one initial attempt and retries after 2s, 4s, and 8s. `{topic}.DLQ` keeps the original key, frame, and headers; only `x-original-topic`, `x-exception-message`, `x-failed-at`, and `x-retry-count` are added or replaced.
- The source offset is manually acknowledged only after handler success or confirmed DLQ publishing. A DLQ publish failure leaves it unacknowledged for redelivery.

Services retain transactional-outbox and idempotent-handler responsibilities; this library provides at-least-once transport only.

## HTTP protobuf JSON

Servlet web apps that depend on Spring MVC get `ProtobufJsonHttpMessageConverter` via auto-config (`ProtobufJsonWebConfig`). Controllers may use generated protobuf messages as `@RequestBody` / return types with snake_case proto field names (`preservingProtoFieldNames`). Plans (and Member later) share this binder; service DTO type packages stay out of common-java.

## Dependency setup

Use only a published, immutable version:

```groovy
dependencies {
    implementation 'com.gym:common-java:2.0.1'
}
```

The package is hosted at `https://maven.pkg.github.com/pploc/common-java`. Development and release verification resolve dependencies without `mavenLocal()`.

## Verification

```bash
./gradlew clean check jacocoTestReport jacocoTestCoverageVerification --no-daemon
./gradlew kafkaContractIntegration --no-daemon
```

`kafkaContractIntegration` requires a Kafka broker, a Schema Registry, and `GYM_PROTO_FIXTURE_PATH`; the repository workflow provisions Confluent Kafka and Schema Registry 7.7.1, verifies the immutable `gym-proto v1.1.0` fixture, and uploads sanitized evidence.
