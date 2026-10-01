package com.bank.kafka.account.service;

import com.bank.kafka.common.BankHeaders;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Thin wrapper around {@link KafkaTemplate} that adds tracing headers and logs the
 * outcome of every send through the asynchronous callback.
 */
@Component
public class EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(EventPublisher.class);
    private static final String SOURCE = "account-service";

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaTemplate<String, Object> transactionalKafkaTemplate;

    public EventPublisher(KafkaTemplate<String, Object> kafkaTemplate,
                          @Qualifier("transactionalKafkaTemplate")
                          KafkaTemplate<String, Object> transactionalKafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.transactionalKafkaTemplate = transactionalKafkaTemplate;
    }

    /** Asynchronous, idempotent send. The key decides the partition and therefore the ordering scope. */
    public CompletableFuture<SendResult<String, Object>> publish(String topic, String key, Object event,
                                                                 String eventType) {
        ProducerRecord<String, Object> record = record(topic, key, event, eventType, UUID.randomUUID().toString());
        return kafkaTemplate.send(record).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish {} key={} to {}", eventType, key, topic, ex);
            } else {
                log.info("Published {} key={} -> {}-{}@{}", eventType, key, topic,
                        result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            }
        });
    }

    /**
     * Publishes all events in a single Kafka transaction: either every record becomes visible to
     * read_committed consumers or none does (the transaction is aborted on any exception).
     */
    public List<SendResult<String, Object>> publishAtomically(String topic, List<KeyedEvent> events,
                                                              String eventType) {
        String correlationId = UUID.randomUUID().toString();
        return transactionalKafkaTemplate.executeInTransaction(ops -> {
            List<CompletableFuture<SendResult<String, Object>>> futures = events.stream()
                    .map(e -> ops.send(record(topic, e.key(), e.event(), eventType, correlationId)))
                    .toList();
            List<SendResult<String, Object>> results = futures.stream().map(CompletableFuture::join).toList();
            log.info("Committed transaction {} with {} {} events", correlationId, results.size(), eventType);
            return results;
        });
    }

    private static ProducerRecord<String, Object> record(String topic, String key, Object event, String eventType,
                                                         String correlationId) {
        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, key, event);
        record.headers()
                .add(new RecordHeader(BankHeaders.CORRELATION_ID, bytes(correlationId)))
                .add(new RecordHeader(BankHeaders.SOURCE_SYSTEM, bytes(SOURCE)))
                .add(new RecordHeader(BankHeaders.EVENT_TYPE, bytes(eventType)));
        return record;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    public record KeyedEvent(String key, Object event) {
    }
}
