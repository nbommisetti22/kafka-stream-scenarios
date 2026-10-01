package com.bank.kafka.account.api;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.support.SendResult;

public final class Responses {

    /** Where the event landed: useful to show partitioning by key. */
    public record PublishedEvent(String id, String topic, int partition, long offset) {

        public static PublishedEvent of(String id, SendResult<String, Object> result) {
            RecordMetadata md = result.getRecordMetadata();
            return new PublishedEvent(id, md.topic(), md.partition(), md.offset());
        }
    }

    private Responses() {
    }
}
