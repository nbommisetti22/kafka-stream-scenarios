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
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Collection;

@Configuration(proxyBeanMethods = false)
public class ListenerConfig {

    private static final Logger log = LoggerFactory.getLogger(ListenerConfig.class);

    /**
     * Blocking retries with back-off for all record and batch listeners (the {@code @RetryableTopic}
     * listener has its own non-blocking strategy). After the retries the record is logged and skipped:
     * a receipt or e-mail is not worth stopping the partition for.
     */
    @Bean
    public DefaultErrorHandler errorHandler() {
        return new DefaultErrorHandler(new FixedBackOff(1000L, 3L));
    }

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
        configurer.configure(factory, consumerFactory);   // also applies the error handler bean
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
