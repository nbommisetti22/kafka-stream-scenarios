package com.bank.kafka.ledger.config;

import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;

@Configuration(proxyBeanMethods = false)
public class StreamsConfig {

    private static final Logger log = LoggerFactory.getLogger(StreamsConfig.class);

    /**
     * Operational hooks: log every state transition (REBALANCING -> RUNNING ...) and replace a
     * stream thread that died from an unexpected exception instead of shutting the app down.
     */
    @Bean
    public StreamsBuilderFactoryBeanConfigurer ledgerStreamsCustomizer() {
        return factoryBean -> {
            factoryBean.setStateListener((newState, oldState) ->
                    log.info("Kafka Streams state {} -> {}", oldState, newState));
            factoryBean.setStreamsUncaughtExceptionHandler(ex -> {
                log.error("Stream thread failed, replacing it", ex);
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
            });
        };
    }
}
