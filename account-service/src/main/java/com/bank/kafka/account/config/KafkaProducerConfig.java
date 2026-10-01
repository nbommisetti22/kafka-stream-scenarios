package com.bank.kafka.account.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.Map;

/**
 * Two producers with different guarantees:
 *
 * <ol>
 *   <li><b>Idempotent producer</b> ({@code kafkaTemplate}) - {@code enable.idempotence=true},
 *       {@code acks=all}: retries never create duplicates and per-partition order is kept.
 *       Used for single events.</li>
 *   <li><b>Transactional producer</b> ({@code transactionalKafkaTemplate}) - sends a group of
 *       records atomically. Consumers with {@code isolation.level=read_committed} see either
 *       all of them or none. Used for bulk payment files.</li>
 * </ol>
 *
 * Common producer settings (acks, idempotence, compression, batching) come from
 * {@code spring.kafka.producer.*} in application.yml.
 */
@Configuration(proxyBeanMethods = false)
public class KafkaProducerConfig {

    @Bean
    @Primary
    public ProducerFactory<String, Object> producerFactory(KafkaProperties properties) {
        return new DefaultKafkaProducerFactory<>(producerProps(properties));
    }

    @Bean
    @Primary
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        KafkaTemplate<String, Object> template = new KafkaTemplate<>(producerFactory);
        template.setObservationEnabled(true);
        return template;
    }

    @Bean
    public ProducerFactory<String, Object> transactionalProducerFactory(KafkaProperties properties) {
        DefaultKafkaProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(producerProps(properties));
        // Each producer instance gets prefix + suffix as transactional.id. On restart the broker
        // fences ("zombie fencing") any older producer that is still alive with the same id.
        factory.setTransactionIdPrefix("account-service-tx-");
        return factory;
    }

    @Bean
    public KafkaTemplate<String, Object> transactionalKafkaTemplate(
            @Qualifier("transactionalProducerFactory") ProducerFactory<String, Object> transactionalProducerFactory) {
        return new KafkaTemplate<>(transactionalProducerFactory);
    }

    private static Map<String, Object> producerProps(KafkaProperties properties) {
        return properties.buildProducerProperties(null);
    }
}
