package com.bank.kafka.processor.listener;

import com.bank.kafka.common.BankHeaders;
import com.bank.kafka.common.Topics;
import com.bank.kafka.common.event.TransactionEvent;
import com.bank.kafka.processor.validation.DeduplicationCache;
import com.bank.kafka.processor.validation.TransactionValidator;
import com.bank.kafka.processor.validation.ValidationResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * <b>Exactly-once consume-transform-produce.</b>
 *
 * <p>Because {@code spring.kafka.producer.transaction-id-prefix} is set, Spring Boot creates a
 * {@code KafkaTransactionManager} and the listener container starts a Kafka transaction before
 * invoking this method. The record sent below and the consumer offset of the input record are
 * committed atomically. Downstream consumers use {@code isolation.level=read_committed}, so they
 * never see output of an aborted attempt.
 *
 * <p>{@code concurrency = 3} starts three consumers in the same group: one per partition.
 */
@Component
public class TransactionRequestListener {

    private static final Logger log = LoggerFactory.getLogger(TransactionRequestListener.class);

    private final TransactionValidator validator;
    private final DeduplicationCache dedup;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public TransactionRequestListener(TransactionValidator validator, DeduplicationCache dedup,
                                      KafkaTemplate<String, Object> kafkaTemplate) {
        this.validator = validator;
        this.dedup = dedup;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(
            id = "transaction-validator",
            topics = Topics.TRANSACTIONS_REQUESTED,
            concurrency = "${banking.processor.concurrency:3}",
            properties = "spring.json.value.default.type=com.bank.kafka.common.event.TransactionEvent")
    public void onTransactionRequested(ConsumerRecord<String, TransactionEvent> record) {
        TransactionEvent txn = record.value();
        if (dedup.isDuplicate(txn.transactionId())) {
            log.warn("Duplicate transaction {} ignored", txn.transactionId());
            return;
        }

        ValidationResult result = validator.validate(txn); // may throw -> rollback -> retry / DLT
        if (result.valid()) {
            send(record, Topics.TRANSACTIONS_VALIDATED, txn.validated());
            log.info("Transaction {} validated ({} {} {})", txn.transactionId(), txn.type(), txn.amount(),
                    txn.currency());
        } else {
            send(record, Topics.TRANSACTIONS_REJECTED, txn.rejected(result.reason()));
            log.info("Transaction {} rejected: {}", txn.transactionId(), result.reason());
        }
        markProcessedOnCommit(txn.transactionId());
    }

    private void send(ConsumerRecord<String, TransactionEvent> in, String topic, TransactionEvent out) {
        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, in.key(), out);
        // Propagate tracing headers so the request can be followed end to end.
        for (String name : new String[]{BankHeaders.CORRELATION_ID, BankHeaders.SOURCE_SYSTEM}) {
            Header header = in.headers().lastHeader(name);
            if (header != null) {
                record.headers().add(header);
            }
        }
        kafkaTemplate.send(record);
    }

    /** Only remember the id once the Kafka transaction actually commits. */
    private void markProcessedOnCommit(String transactionId) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    dedup.markProcessed(transactionId);
                }
            });
        } else {
            dedup.markProcessed(transactionId);
        }
    }
}
