package com.bank.kafka.notification;

import com.bank.kafka.common.Topics;
import com.bank.kafka.common.event.FraudAlert;
import com.bank.kafka.common.event.Severity;
import com.bank.kafka.common.event.TransactionEvent;
import com.bank.kafka.common.event.TransactionType;
import com.bank.kafka.common.serde.JsonSerde;
import com.bank.kafka.notification.gateway.Notification;
import com.bank.kafka.notification.gateway.NotificationGateway;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "banking.notifications.simulated-failure-rate=0",
        "server.port=0"})
@EmbeddedKafka(partitions = 3)
class NotificationServiceIntegrationTest {

    private static final JsonSerde<Object> JSON = new JsonSerde<>(Object.class);

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Autowired
    private NotificationGateway gateway;

    @Test
    void notifiesOnPostedRejectedAndFraudEvents() throws Exception {
        try (Producer<String, byte[]> producer = new DefaultKafkaProducerFactory<>(KafkaTestUtils.producerProps(broker),
                new StringSerializer(), new ByteArraySerializer()).createProducer()) {
            TransactionEvent base = TransactionEvent.request("ACC-N", TransactionType.PAYMENT, new BigDecimal("20"),
                    "USD", "CARD", "US", "Coffee");
            producer.send(new ProducerRecord<>(Topics.TRANSACTIONS_POSTED, "ACC-N",
                    JSON.serializer().serialize("t", base.posted(new BigDecimal("80")))));
            producer.send(new ProducerRecord<>(Topics.TRANSACTIONS_REJECTED, "ACC-N",
                    JSON.serializer().serialize("t", base.rejected("INSUFFICIENT_FUNDS: available 0 USD"))));
            producer.send(new ProducerRecord<>(Topics.FRAUD_ALERTS, "ACC-N", JSON.serializer().serialize("t",
                    FraudAlert.of("ACC-N", "t1", "LARGE_AMOUNT", Severity.HIGH, "too big", Instant.now()))));
            producer.flush();
        }

        awaitNotification(n -> n.channel().equals("PUSH") && n.message().contains("New balance: 80"));
        awaitNotification(n -> n.channel().equals("EMAIL") && n.message().contains("INSUFFICIENT_FUNDS"));
        awaitNotification(n -> n.channel().equals("SMS") && n.message().contains("too big"));
    }

    private void awaitNotification(Predicate<Notification> match) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            List<Notification> recent = gateway.recent();
            if (recent.stream().anyMatch(match)) {
                return;
            }
            Thread.sleep(100);
        }
        assertThat(gateway.recent()).anyMatch(match);
    }
}
