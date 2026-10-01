package com.bank.kafka.processor;

import com.bank.kafka.common.config.BankingTopicsConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@ConfigurationPropertiesScan
@Import(BankingTopicsConfig.class)
public class TransactionProcessorApplication {

    public static void main(String[] args) {
        SpringApplication.run(TransactionProcessorApplication.class, args);
    }
}
