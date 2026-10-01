package com.bank.kafka.common.serde;

import com.bank.kafka.common.event.TransactionEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Event-time semantics: windows and joins use the time the customer actually made the
 * transaction (carried in the payload), not the time Kafka received the record. Falls back
 * to the record timestamp when the payload has no timestamp.
 */
public class TransactionTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if (record.value() instanceof TransactionEvent txn && txn.timestamp() != null) {
            return txn.timestamp().toEpochMilli();
        }
        return record.timestamp() >= 0 ? record.timestamp() : partitionTime;
    }
}
