package com.gym.common.kafka;

import build.buf.protovalidate.ValidationResult;
import build.buf.protovalidate.Validator;
import build.buf.protovalidate.exceptions.ValidationException;
import com.google.protobuf.Message;

import java.util.Map;
import java.util.Set;

/** Frozen Kafka contract shared by publishing and raw delivery paths. */
public final class KafkaContract {
    public static final String HEADER_EVENT_TYPE = "event-type";
    public static final String HEADER_SOURCE = "source";
    public static final String HEADER_TIMESTAMP = "timestamp";
    public static final String HEADER_EVENT_ID = "event-id";
    public static final String HEADER_TRACEPARENT = "traceparent";
    public static final String HEADER_TRACESTATE = "tracestate";
    public static final String HEADER_TRACE_ID = "x-trace-id";

    public static final String HEADER_ORIGINAL_TOPIC = "x-original-topic";
    public static final String HEADER_EXCEPTION_MESSAGE = "x-exception-message";
    public static final String HEADER_FAILED_AT = "x-failed-at";
    public static final String HEADER_RETRY_COUNT = "x-retry-count";

    public static final Map<String, String> TOPIC_TYPES = Map.ofEntries(
            Map.entry("identity.user.registered.v1", "events.v1.UserRegisteredEvent"),
            Map.entry("identity.user.suspended.v1", "events.v1.UserSuspendedEvent"),
            Map.entry("identity.user.role-changed.v1", "events.v1.UserRoleChangedEvent"),
            Map.entry("identity.email.verification-requested.v1", "events.v1.EmailVerificationRequestedEvent"),
            Map.entry("payment.completed.v1", "events.v1.PaymentCompletedEvent"),
            Map.entry("membership.activated.v1", "events.v1.MembershipActivatedEvent"),
            Map.entry("membership.paused.v1", "events.v1.MembershipPausedEvent"),
            Map.entry("membership.resumed.v1", "events.v1.MembershipResumedEvent"),
            Map.entry("membership.expiring-soon.v1", "events.v1.MembershipExpiringSoonEvent"),
            Map.entry("membership.expired.v1", "events.v1.MembershipExpiredEvent"),
            Map.entry("checkin.recorded.v1", "events.v1.CheckInRecordedEvent")
    );

    public static final Set<String> REQUIRED_HEADERS = Set.of(
            HEADER_EVENT_TYPE,
            HEADER_SOURCE,
            HEADER_TIMESTAMP,
            HEADER_EVENT_ID,
            HEADER_TRACEPARENT
    );

    public static final Set<String> RESERVED_HEADERS = Set.of(
            HEADER_EVENT_TYPE,
            HEADER_SOURCE,
            HEADER_TIMESTAMP,
            HEADER_EVENT_ID,
            HEADER_TRACEPARENT,
            HEADER_TRACESTATE,
            HEADER_TRACE_ID
    );

    private static final Validator VALIDATOR = new Validator();

    private KafkaContract() {
    }

    public static void requireFrozenPair(String topic, Message message) {
        String expected = TOPIC_TYPES.get(topic);
        if (expected == null || !expected.equals(message.getDescriptorForType().getFullName())) {
            throw new IllegalArgumentException("Kafka topic and concrete Protobuf type are not a frozen contract pair");
        }
    }

    public static void requireValid(Message message) {
        try {
            ValidationResult result = VALIDATOR.validate(message);
            if (!result.isSuccess()) {
                throw new IllegalArgumentException("Kafka payload failed Protovalidate constraints: " + result);
            }
        } catch (ValidationException exception) {
            throw new IllegalArgumentException("Kafka payload failed Protovalidate constraints", exception);
        }
    }

    public static String subjectFor(String topic) {
        if (!TOPIC_TYPES.containsKey(topic)) {
            throw new IllegalArgumentException("Kafka topic is not part of the frozen contract");
        }
        return topic + "-value";
    }
}
