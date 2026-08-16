# ADR 0001: Frozen platform contracts

- **Status:** Accepted
- **Date:** 2026-08-03

## Context

The Go and Java shared libraries must consume the same identity, tracing, Kafka,
and gateway contracts. `gym-proto/contracts/v1` is the canonical machine-readable
source; this ADR records the Java decision boundary before transport changes.

## Decision

### Trusted claims and identity

Kong validates external JWTs, strips client-supplied trusted headers, and injects
`x-user-id`, `x-user-role`, `x-gym-id`, and `x-membership-status`. Downstream
libraries trim and normalize values and reject conflicting duplicates. Claims
do not establish trust unless the request passed through the authenticated Kong
boundary or a separately verified workload channel.

Canonical roles are `CUSTOMER`, `TRAINER`, `ADMIN`, and `SUPER_ADMIN`. Public
registration creates only `CUSTOMER`; elevated roles require protected
administration or controlled out-of-band provisioning. Workload identities are
not user roles and never appear in `x-user-role`.

Canonical membership statuses are `NONE`, `ACTIVE`, `PAUSED`, and `EXPIRED`.
New customers and non-customer roles use `NONE`. Ordinary authenticated methods
accept any known status. Membership-gated methods require `ACTIVE` and fail
closed on a missing, blank, malformed, conflicting, or unknown status.

### Tracing

Valid W3C `traceparent` and optional `tracestate` are authoritative. `x-trace-id`
is compatibility correlation only: it is used only when valid W3C context is
absent and never fabricates an OpenTelemetry parent.

### Workload identity

Identifier-to-Member calls use mTLS. The certificate identity/SAN identifies
`ms-gym-identifier`; Member authorizes the verified peer; NetworkPolicy allows
only intended callers on native gRPC port `50051`. Observability metadata such
as `x-service-id` is never trusted independently of the verified peer.

### Kafka

The current generation uses eleven `.v1` topics and `<topic>-value` subjects in
`gym-proto/contracts/v1/kafka/wire-format.json`, `TopicNameStrategy`, `BACKWARD`
compatibility, and production `auto.register.schemas=false`. Values are
Confluent-framed concrete Protobuf messages with no envelope. Canonical headers
are `event-type`, `source`, `timestamp`, `event-id`, `traceparent`, and optional
`tracestate`; `x-trace-id` is read-only fallback correlation. New producers emit
no `x-event-*` headers. Delivery is at-least-once, DLQ topics use `{topic}.DLQ`,
and the default Member group is `ms-gym-member-v2`.

Because Kafka is greenfield, there is no JSON/envelope migration, dual read or
write, offset conversion, or legacy-topic adapter.

## Consequences

Existing Java tests and transport code that use `MEMBER`, accept unverified
headers, wrap events in JSON envelopes, or emit legacy headers are implementation
work for the next phase and cannot define the contract. Missing owner approval
remains pending in the contract manifest.
