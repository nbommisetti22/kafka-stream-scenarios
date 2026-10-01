package com.bank.kafka.fraud.topology;

import com.bank.kafka.common.event.FraudAlert;
import com.bank.kafka.common.event.Severity;
import com.bank.kafka.common.event.TransactionEvent;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.Instant;

/**
 * Custom stateful rule with the <b>Processor API</b>: remember the last country each account
 * transacted from, and alert when a new transaction comes from another country too soon after.
 * Only forwards a record when an alert is raised (acts as filter + map in one step).
 */
public class ImpossibleTravelProcessor implements FixedKeyProcessor<String, TransactionEvent, FraudAlert> {

    public static final String RULE = "IMPOSSIBLE_TRAVEL";

    private final String storeName;
    private final Duration window;
    private FixedKeyProcessorContext<String, FraudAlert> context;
    private KeyValueStore<String, TransactionEvent> lastSeen;

    public ImpossibleTravelProcessor(String storeName, Duration window) {
        this.storeName = storeName;
        this.window = window;
    }

    @Override
    public void init(FixedKeyProcessorContext<String, FraudAlert> context) {
        this.context = context;
        this.lastSeen = context.getStateStore(storeName);
    }

    @Override
    public void process(FixedKeyRecord<String, TransactionEvent> record) {
        TransactionEvent current = record.value();
        if (current == null || current.country() == null) {
            return;
        }
        Instant currentTime = Instant.ofEpochMilli(record.timestamp());
        TransactionEvent previous = lastSeen.get(record.key());

        if (previous != null && !previous.country().equals(current.country())) {
            Duration gap = Duration.between(previous.timestamp(), currentTime).abs();
            if (gap.compareTo(window) <= 0) {
                context.forward(record.withValue(FraudAlert.of(record.key(), current.transactionId(), RULE,
                        Severity.CRITICAL,
                        "Used in " + previous.country() + " and " + current.country() + " within "
                                + gap.toMinutes() + " min",
                        currentTime)));
            }
        }
        // Only move forward in time: an out-of-order older event must not replace a newer one.
        if (previous == null || !currentTime.isBefore(previous.timestamp())) {
            lastSeen.put(record.key(), current.withTimestamp(currentTime));
        }
    }
}
