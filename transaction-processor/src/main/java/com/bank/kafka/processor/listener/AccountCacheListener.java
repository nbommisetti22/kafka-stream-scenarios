package com.bank.kafka.processor.listener;

import com.bank.kafka.common.Topics;
import com.bank.kafka.common.event.AccountEvent;
import com.bank.kafka.processor.validation.AccountCache;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Rebuilds a local cache from a <b>compacted topic</b>.
 *
 * <p>Every instance uses its <b>own random consumer group</b>, so every instance receives
 * <i>all</i> partitions (a "broadcast" pattern, similar to a Kafka Streams GlobalKTable). On
 * assignment it <b>seeks to the beginning</b> ({@link AbstractConsumerSeekAware}) so the full
 * table is replayed at every start-up. Tombstones (null values) remove the entry.
 */
@Component
public class AccountCacheListener extends AbstractConsumerSeekAware {

    private static final Logger log = LoggerFactory.getLogger(AccountCacheListener.class);

    private final AccountCache cache;

    public AccountCacheListener(AccountCache cache) {
        this.cache = cache;
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        super.onPartitionsAssigned(assignments, callback);
        callback.seekToBeginning(assignments.keySet());
        log.info("Replaying {} from the beginning", assignments.keySet());
    }

    @KafkaListener(
            id = "account-cache-loader",
            topics = Topics.ACCOUNTS,
            groupId = "transaction-processor-account-cache-#{T(java.util.UUID).randomUUID()}",
            properties = "spring.json.value.default.type=com.bank.kafka.common.event.AccountEvent")
    public void onAccount(ConsumerRecord<String, AccountEvent> record) {
        if (record.value() == null) {
            cache.remove(record.key());
            log.info("Account {} removed (tombstone)", record.key());
        } else {
            cache.put(record.value());
            log.info("Account {} cached with status {} (cache size {})", record.key(),
                    record.value().status(), cache.size());
        }
    }
}
