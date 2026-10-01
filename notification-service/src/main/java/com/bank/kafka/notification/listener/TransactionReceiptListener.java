package com.bank.kafka.notification.listener;

import com.bank.kafka.common.Topics;
import com.bank.kafka.common.event.TransactionEvent;
import com.bank.kafka.notification.gateway.NotificationGateway;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * <b>Batch consumption + manual commit.</b> Sends a push receipt for every posted transaction
 * and commits the offsets of the batch only once all receipts were handed to the gateway.
 *
 * <p>This listener uses its own consumer group ({@code notification-receipts}), so it receives
 * every posted transaction independently of the fraud and ledger services reading the same topic.
 */
@Component
public class TransactionReceiptListener {

    private static final Logger log = LoggerFactory.getLogger(TransactionReceiptListener.class);

    private final NotificationGateway gateway;

    public TransactionReceiptListener(NotificationGateway gateway) {
        this.gateway = gateway;
    }

    @KafkaListener(
            id = "transaction-receipts",
            groupId = "notification-receipts",
            topics = Topics.TRANSACTIONS_POSTED,
            containerFactory = "batchFactory",
            properties = "spring.json.value.default.type=com.bank.kafka.common.event.TransactionEvent")
    public void onPostedBatch(List<ConsumerRecord<String, TransactionEvent>> records, Acknowledgment ack) {
        log.info("Received batch of {} posted transactions", records.size());
        for (ConsumerRecord<String, TransactionEvent> record : records) {
            TransactionEvent txn = record.value();
            if (txn == null) {
                continue;
            }
            gateway.send("PUSH", "owner-of-" + txn.accountId(), txn.accountId(),
                    String.format("%s of %s %s posted. New balance: %s %s", txn.type(), txn.amount(),
                            txn.currency(), txn.balanceAfter(), txn.currency()));
        }
        ack.acknowledge();
    }
}
