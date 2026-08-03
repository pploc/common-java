# Common Java G2 status

## Current implementation

`common-java` uses Java 26, Spring Boot 4, and the published `com.gym.proto:gym-proto-java:1.1.0` contract. The `develop` version is `2.0.0-SNAPSHOT`; no G2 package or RC has been published.

Kafka uses concrete Protobuf publication, Confluent Schema Registry `TopicNameStrategy`, and lookup-only clients (`auto.register.schemas=false`). Raw ingress is decoded through `RawDeliveryCoordinator`; `RawKafkaListenerAdapter` bridges a service-owned Spring listener to that path. The coordinator retries after 2s, 4s, and 8s and acknowledges only after successful handling or confirmed raw DLQ publication.

The legacy JSON `EventEnvelope` transport was removed. `verifyNoLegacyKafkaTransport` guards against reintroduction.

## G2 verification

```bash
./gradlew clean check jacocoTestReport jacocoTestCoverageVerification --no-daemon
./gradlew kafkaContractIntegration --no-daemon
```

The reusable Kafka contract workflow provisions Kafka and Schema Registry 7.7.1, validates the immutable `gym-proto v1.1.0` fixture input, verifies Registry non-mutation, and uploads sanitized reports.

## Deferred work

- Legal POM metadata requires repository-owner input.
- G3 creates immutable RC artifacts and runs the Java-to-Go/Go-to-Java matrix.
- No ordinary service adopts this source line before G3/G4.
