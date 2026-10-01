package com.bank.kafka.notification.listener;

import com.bank.kafka.common.Topics;
import com.bank.kafka.common.event.FraudAlert;
import com.bank.kafka.notification.gateway.NotificationGateway;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.SameIntervalTopicReuseStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

/**
 * <b>Non-blocking retries</b> with {@link RetryableTopic}.
 *
 * <p>If the SMS provider is down, the failed alert is moved to a retry topic
 * ({@code bank.fraud.alerts-retry-0}, {@code -retry-1}, ...) and consumed again after a delay,
 * while the main topic keeps flowing. After the last attempt it lands in
 * {@code bank.fraud.alerts-dlt} and {@link #onDeadLetter} is invoked. Compare with the blocking
 * retries of the transaction-processor, which preserve ordering but stall the partition.
 */
@Component
public class FraudAlertListener {

    private static final Logger log = LoggerFactory.getLogger(FraudAlertListener.class);

    private final NotificationGateway gateway;

    public FraudAlertListener(NotificationGateway gateway) {
        this.gateway = gateway;
    }

    @RetryableTopic(
            attempts = "4",
            backoff = @Backoff(delay = 1000, multiplier = 2.0, maxDelay = 10000),
            numPartitions = "${banking.topics.partitions:3}",
            replicationFactor = "${banking.topics.replicas:1}",
            sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.SINGLE_TOPIC,
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            exclude = {IllegalArgumentException.class})
    @KafkaListener(
            id = "fraud-alert-notifier",
            topics = Topics.FRAUD_ALERTS,
            properties = "spring.json.value.default.type=com.bank.kafka.common.event.FraudAlert")
    public void onFraudAlert(FraudAlert alert, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.info("Fraud alert {} on {} received from {}", alert.rule(), alert.accountId(), topic);
        gateway.send("SMS", "owner-of-" + alert.accountId(), alert.accountId(),
                "Security alert [" + alert.severity() + "] " + alert.description()
                        + ". Reply NO if this was not you.");
    }

    @DltHandler
    public void onDeadLetter(ConsumerRecord<String, FraudAlert> record,
                             @Header(KafkaHeaders.EXCEPTION_MESSAGE) String error) {
        log.error("Could not notify customer about alert on {} after all retries ({}). Escalating to call centre.",
                record.key(), error);
    }
}
