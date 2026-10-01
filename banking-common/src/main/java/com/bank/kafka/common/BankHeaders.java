package com.bank.kafka.common;

/** Custom Kafka record headers used for tracing and routing across services. */
public final class BankHeaders {

    /** Correlates every event produced for one business request across all services. */
    public static final String CORRELATION_ID = "x-correlation-id";

    /** The system / channel that originally produced the event. */
    public static final String SOURCE_SYSTEM = "x-source-system";

    /** Logical event type, lets consumers route without deserializing the payload. */
    public static final String EVENT_TYPE = "x-event-type";

    private BankHeaders() {
    }
}
