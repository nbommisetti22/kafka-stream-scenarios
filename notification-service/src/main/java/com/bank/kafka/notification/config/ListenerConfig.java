package com.bank.kafka.notification.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.Collection;

@Configuration(proxyBeanMethods = false)
public class ListenerConfig {

    private static final Logger log = LoggerFactory.getLogger(ListenerConfig.class);

    /**
     * <b>Batch listener with manual acknowledgment.</b> The listener receives up to
     * {@code max.poll.records} records at once and acknowledges the whole batch only after all
     * receipts were dispatched - at-least-once delivery with fewer offset commits.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> batchFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, consumerFactory);
        factory.setBatchListener(true);
        factory.setConcurrency(3);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.getContainerProperties().setConsumerRebalanceListener(new LoggingRebalanceListener());
        return factory;
    }

    /** Shows partition (re)assignment of a consumer group, e.g. when a new instance starts. */
    static class LoggingRebalanceListener implements ConsumerAwareRebalanceListener {

        @Override
        public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
            log.info("Partitions revoked: {}", partitions);
        }

        @Override
        public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
            log.info("Partitions assigned: {}", partitions);
        }
    }
}
