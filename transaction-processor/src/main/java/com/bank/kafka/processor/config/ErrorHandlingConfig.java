package com.bank.kafka.processor.config;

import com.bank.kafka.processor.validation.AccountNotFoundException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AfterRollbackProcessor;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultAfterRollbackProcessor;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

/**
 * Error handling for the <b>transactional</b> listener container.
 *
 * <p>When the listener throws, the Kafka transaction (output records + consumer offsets) is
 * aborted and the {@link AfterRollbackProcessor} decides what to do with the failed record:
 *
 * <ul>
 *   <li><b>Transient errors</b> (e.g. {@link AccountNotFoundException}: the account event has not
 *       reached our cache yet) are retried with exponential back-off - a <i>blocking</i> retry
 *       that preserves partition ordering.</li>
 *   <li>After retries are exhausted, or immediately for non-retryable errors such as a
 *       {@code DeserializationException} (poison pill), the record is published to
 *       {@code <topic>.DLT} on the <i>same partition</i>, with exception details in headers, and
 *       its offset is committed so the partition is not blocked.</li>
 * </ul>
 */
@Configuration(proxyBeanMethods = false)
public class ErrorHandlingConfig {

    private static final Logger log = LoggerFactory.getLogger(ErrorHandlingConfig.class);

    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(KafkaTemplate<Object, Object> template,
                                                                       ProcessorProperties props) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template,
                (record, ex) -> {
                    log.error("Sending record {}-{}@{} to DLT: {}", record.topic(), record.partition(),
                            record.offset(), ex.getMessage());
                    return new TopicPartition(record.topic() + props.dltSuffix(), record.partition());
                });
        recoverer.setFailIfSendResultIsError(true);
        return recoverer;
    }

    @Bean
    public AfterRollbackProcessor<Object, Object> afterRollbackProcessor(DeadLetterPublishingRecoverer recoverer,
                                                                         KafkaTemplate<Object, Object> template,
                                                                         ProcessorProperties props) {
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(props.maxRetries());
        backOff.setInitialInterval(props.initialBackoff().toMillis());
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(10_000);

        // commitRecovered=true: after DLT publication the offset is committed in a transaction too.
        DefaultAfterRollbackProcessor<Object, Object> processor =
                new DefaultAfterRollbackProcessor<>(recoverer, backOff, template, true);
        processor.addNotRetryableExceptions(IllegalArgumentException.class, NullPointerException.class);
        return processor;
    }
}
