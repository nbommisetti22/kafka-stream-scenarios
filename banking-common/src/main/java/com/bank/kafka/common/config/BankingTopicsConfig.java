package com.bank.kafka.common.config;

import com.bank.kafka.common.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * Declares every platform topic. Spring's {@link KafkaAdmin} creates missing topics at
 * start-up, so whichever service boots first provisions the cluster. Import it with
 * {@code @Import(BankingTopicsConfig.class)}.
 *
 * <ul>
 *   <li>Entity topics (customers, accounts, balances) are <b>compacted</b>: Kafka keeps the
 *       latest value per key forever, so a new consumer can rebuild full state.</li>
 *   <li>Event topics use time based retention.</li>
 *   <li>All co-partitioned topics share the same partition count.</li>
 * </ul>
 */
@Configuration(proxyBeanMethods = false)
public class BankingTopicsConfig {

    @Value("${banking.topics.partitions:3}")
    private int partitions;

    @Value("${banking.topics.replicas:1}")
    private int replicas;

    @Bean
    public KafkaAdmin.NewTopics bankingTopics() {
        return new KafkaAdmin.NewTopics(
                compacted(Topics.CUSTOMERS),
                compacted(Topics.ACCOUNTS),
                compacted(Topics.BALANCES),
                events(Topics.TRANSACTIONS_REQUESTED, "604800000"),          // 7 days
                events(Topics.TRANSACTIONS_REQUESTED_DLT, "2592000000"),     // 30 days, for investigation
                events(Topics.TRANSACTIONS_VALIDATED, "604800000"),
                events(Topics.TRANSACTIONS_POSTED, "2592000000"),
                events(Topics.TRANSACTIONS_REJECTED, "2592000000"),
                events(Topics.TRANSACTIONS_ENRICHED, "604800000"),
                events(Topics.ACCOUNT_ACTIVITY_SUMMARY, "2592000000"),
                events(Topics.FRAUD_ALERTS, "2592000000"),
                events(Topics.FRAUD_CASES, "-1"));                            // keep forever (audit)
    }

    private NewTopic compacted(String name) {
        return TopicBuilder.name(name)
                .partitions(partitions)
                .replicas(replicas)
                .compact()
                .config(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "60000")
                .build();
    }

    private NewTopic events(String name, String retentionMs) {
        return TopicBuilder.name(name)
                .partitions(partitions)
                .replicas(replicas)
                .config(TopicConfig.RETENTION_MS_CONFIG, retentionMs)
                .build();
    }
}
