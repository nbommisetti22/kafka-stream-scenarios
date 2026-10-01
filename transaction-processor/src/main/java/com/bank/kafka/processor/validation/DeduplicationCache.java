package com.bank.kafka.processor.validation;

import com.bank.kafka.processor.config.ProcessorProperties;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Bounded LRU set of processed transaction ids.
 *
 * <p>Exactly-once in Kafka covers the pipeline <i>inside</i> Kafka. If a client resubmits the
 * same payment (double click, mobile retry), that is a new record and must be de-duplicated by
 * business key. Production systems back this with a durable store (DB unique key, or a Kafka
 * Streams state store); an in-memory LRU keeps the example self-contained.
 */
@Component
public class DeduplicationCache {

    private final Set<String> processed;

    public DeduplicationCache(ProcessorProperties props) {
        int max = props.dedupCacheSize();
        this.processed = Collections.synchronizedSet(Collections.newSetFromMap(new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                return size() > max;
            }
        }));
    }

    public boolean isDuplicate(String transactionId) {
        return processed.contains(transactionId);
    }

    public void markProcessed(String transactionId) {
        processed.add(transactionId);
    }
}
