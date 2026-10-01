package com.bank.kafka.notification.listener;

import com.bank.kafka.common.BankHeaders;
import com.bank.kafka.common.Topics;
import com.bank.kafka.common.event.TransactionEvent;
import com.bank.kafka.notification.gateway.NotificationGateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/** Reads payload plus record metadata and custom headers via {@link Header} injection. */
@Component
public class RejectedTransactionListener {

    private static final Logger log = LoggerFactory.getLogger(RejectedTransactionListener.class);

    private final NotificationGateway gateway;

    public RejectedTransactionListener(NotificationGateway gateway) {
        this.gateway = gateway;
    }

    @KafkaListener(
            id = "rejection-notifier",
            topics = Topics.TRANSACTIONS_REJECTED,
            properties = "spring.json.value.default.type=com.bank.kafka.common.event.TransactionEvent")
    public void onRejected(@Payload TransactionEvent txn,
                           @Header(KafkaHeaders.RECEIVED_KEY) String accountId,
                           @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                           @Header(KafkaHeaders.OFFSET) long offset,
                           @Header(name = BankHeaders.CORRELATION_ID, required = false) byte[] correlationId) {
        log.info("Rejected txn {} (partition {}, offset {}, correlation {})", txn.transactionId(), partition, offset,
                correlationId == null ? "-" : new String(correlationId));
        gateway.send("EMAIL", "owner-of-" + accountId, accountId,
                String.format("Your %s of %s %s was declined: %s", txn.type(), txn.amount(), txn.currency(),
                        txn.reason()));
    }
}
