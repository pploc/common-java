# gym-common-java

Shared Java library for the gym-chain microservices. Built with Java 26 and Spring Boot 4.

## Features

### 1. gRPC Auto-Configured Interceptors
Auto-configures standard gRPC server interceptor beans for microservices:
- **Tracing**: Integrates OpenTelemetry span propagation.
- **Logging**: Captures request start, end, status, and duration using MDC trace ID context.
- **Metrics**: Records call latency and completions using Micrometer.
- **Authentication/Role Guards**: Injects headers into `GrpcSecurityContext` and enforces `@RequireRole` on methods/classes.
- **Exception Mapping**: Translates `DomainException` to gRPC `Status` codes with header details.

### 2. Kafka Messaging
Synchronous Event Publisher ensuring atomic database-and-message operations.
- **EventEnvelope**: Standardized JSON envelope wrapping Protobuf payloads.
- **Error Handling & DLQ**: Exponential backoff retry handler (3 retries: 2s, 4s, 8s) routing toxic messages to `[topic].DLQ` with diagnostic headers.

### 3. Pagination
- **CursorPage / CursorUtils**: URL-safe base64 keyset pagination helper (supporting compound fields).
- **PageMapper**: Maps Java pagination state to Protobuf response structures.

### 4. Persistence
- **BaseEntity**: Abstract JPA class providing UUID ID generation.
- **AuditListener**: Auto-populates `created_at` and `updated_at` fields.

---

## Getting Started

### Dependency Setup
Add the library dependency to your service `build.gradle`:

```groovy
repositories {
    maven {
        url 'https://maven.pkg.github.com/pploc/common-java'
        credentials {
            username = System.getenv("GITHUB_ACTOR") ?: project.findProperty("gpr.user")
            password = System.getenv("GITHUB_TOKEN") ?: project.findProperty("gpr.key")
        }
    }
}

dependencies {
    implementation 'com.gym:common-java:1.0.0'
}
```

Auto-configurations are loaded automatically via Spring Boot `AutoConfiguration.imports`.

---

## Configuration Properties

Configure the components in `application.yml`:

```yaml
grpc:
  server:
    port: 9090

spring:
  application:
    name: ms-gym-service
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      timeout: 25000
```

---

## Development

### Compilation & Tests
Run Gradle tasks:
```bash
./gradlew compileJava
./gradlew test
```
