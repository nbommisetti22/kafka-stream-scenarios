package com.bank.kafka.ledger;

import com.bank.kafka.common.config.BankingTopicsConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
@ConfigurationPropertiesScan
@Import(BankingTopicsConfig.class)
public class LedgerStreamsApplication {

    public static void main(String[] args) {
        SpringApplication.run(LedgerStreamsApplication.class, args);
    }
}
