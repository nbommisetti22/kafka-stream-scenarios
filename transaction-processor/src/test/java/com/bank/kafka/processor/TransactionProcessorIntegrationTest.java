package com.bank.kafka.processor;

import com.bank.kafka.common.Topics;
import com.bank.kafka.common.event.AccountEvent;
import com.bank.kafka.common.event.AccountStatus;
import com.bank.kafka.common.event.TransactionEvent;
import com.bank.kafka.common.event.TransactionStatus;
import com.bank.kafka.common.event.TransactionType;
import com.bank.kafka.common.serde.JsonSerde;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "banking.processor.max-retries=2",
        "banking.processor.initial-backoff=100ms",
        "banking.processor.concurrency=1",
        "server.port=0"})
@EmbeddedKafka(partitions = 3, brokerProperties = {
        "transaction.state.log.replication.factor=1",
        "transaction.state.log.min.isr=1",
        "offsets.topic.replication.factor=1"})
class TransactionProcessorIntegrationTest {

    private static final JsonSerde<Object> JSON = new JsonSerde<>(Object.class);

    @Autowired
    private EmbeddedKafkaBroker broker;

    private Producer<String, byte[]> producer;
    private Consumer<String, byte[]> consumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(broker);
        producer = new DefaultKafkaProducerFactory<>(producerProps, new StringSerializer(), new ByteArraySerializer())
                .createProducer();

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("it-" + System.nanoTime(), "false", broker);
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consumer = new DefaultKafkaConsumerFactory<>(consumerProps, new StringDeserializer(),
                new ByteArrayDeserializer()).createConsumer();
        consumer.subscribe(List.of(Topics.TRANSACTIONS_VALIDATED, Topics.TRANSACTIONS_REJECTED,
                Topics.TRANSACTIONS_REQUESTED_DLT));

        send(Topics.ACCOUNTS, "ACC-1", json(new AccountEvent("ACC-1", "CUST-1", "CHECKING", "USD",
                AccountStatus.ACTIVE, Instant.now())));
    }

    @AfterEach
    void tearDown() {
        producer.close();
        consumer.close();
    }

    @Test
    void routesValidRejectedAndPoisonRecords() {
        TransactionEvent valid = request("valid-1", "ACC-1", "100.00", "USD");
        TransactionEvent wrongCurrency = request("bad-ccy-1", "ACC-1", "10.00", "JPY");
        send(Topics.TRANSACTIONS_REQUESTED, "ACC-1", json(valid));
        send(Topics.TRANSACTIONS_REQUESTED, "ACC-1", json(valid));              // client resubmission
        send(Topics.TRANSACTIONS_REQUESTED, "ACC-1", json(wrongCurrency));
        byte[] poison = "{not json".getBytes(StandardCharsets.UTF_8);
        send(Topics.TRANSACTIONS_REQUESTED, "ACC-1", poison);
        send(Topics.TRANSACTIONS_REQUESTED, "ACC-404", json(request("ghost-1", "ACC-404", "5.00", "USD")));

        List<ConsumerRecord<String, byte[]>> records = pollUntil(list ->
                count(list, Topics.TRANSACTIONS_VALIDATED) >= 1
                        && count(list, Topics.TRANSACTIONS_REJECTED) >= 1
                        && count(list, Topics.TRANSACTIONS_REQUESTED_DLT) >= 2);

        List<TransactionEvent> validated = values(records, Topics.TRANSACTIONS_VALIDATED);
        assertThat(validated).extracting(TransactionEvent::transactionId).containsExactly("valid-1");
        assertThat(validated.get(0).status()).isEqualTo(TransactionStatus.VALIDATED);

        List<TransactionEvent> rejected = values(records, Topics.TRANSACTIONS_REJECTED);
        assertThat(rejected).extracting(TransactionEvent::transactionId).containsExactly("bad-ccy-1");
        assertThat(rejected.get(0).reason()).startsWith("UNSUPPORTED_CURRENCY");

        List<ConsumerRecord<String, byte[]>> dlt = records.stream()
                .filter(r -> r.topic().equals(Topics.TRANSACTIONS_REQUESTED_DLT)).toList();
        assertThat(dlt).anySatisfy(r -> {
            assertThat(r.value()).isEqualTo(poison);   // raw bytes preserved
            assertThat(headerString(r, KafkaHeaders.DLT_EXCEPTION_FQCN)).endsWith("DeserializationException");
        });
        assertThat(dlt).anySatisfy(r -> {
            assertThat(r.key()).isEqualTo("ACC-404");
            assertThat(headerString(r, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)).endsWith("AccountNotFoundException");
            assertThat(headerString(r, KafkaHeaders.DLT_ORIGINAL_TOPIC)).isEqualTo(Topics.TRANSACTIONS_REQUESTED);
        });
    }

    private List<ConsumerRecord<String, byte[]>> pollUntil(Predicate<List<ConsumerRecord<String, byte[]>>> done) {
        List<ConsumerRecord<String, byte[]>> all = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 30_000;
        while (!done.test(all) && System.currentTimeMillis() < deadline) {
            consumer.poll(Duration.ofMillis(250)).forEach(all::add);
        }
        // Drain a little longer to catch unexpected duplicates.
        long settle = System.currentTimeMillis() + 2_000;
        while (System.currentTimeMillis() < settle) {
            consumer.poll(Duration.ofMillis(250)).forEach(all::add);
        }
        assertThat(done.test(all)).as("expected records within timeout, got %s", all.size()).isTrue();
        return all;
    }

    private static long count(List<ConsumerRecord<String, byte[]>> records, String topic) {
        return records.stream().filter(r -> r.topic().equals(topic)).count();
    }

    private static List<TransactionEvent> values(List<ConsumerRecord<String, byte[]>> records, String topic) {
        JsonSerde<TransactionEvent> serde = new JsonSerde<>(TransactionEvent.class);
        return records.stream().filter(r -> r.topic().equals(topic))
                .map(r -> serde.deserializer().deserialize(topic, r.value())).toList();
    }

    private static String headerString(ConsumerRecord<?, ?> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    private void send(String topic, String key, byte[] value) {
        producer.send(new ProducerRecord<>(topic, key, value));
        producer.flush();
    }

    private static byte[] json(Object value) {
        return JSON.serializer().serialize("any", value);
    }

    private static TransactionEvent request(String id, String account, String amount, String currency) {
        return TransactionEvent.request(account, TransactionType.DEPOSIT, new BigDecimal(amount), currency,
                "ONLINE", "US", null).withTransactionId(id);
    }
}
