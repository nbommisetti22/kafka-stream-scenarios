package com.bank.kafka.common.serde;

import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.support.serializer.JsonSerializer;

/**
 * JSON serializer that passes {@code byte[]} through untouched.
 *
 * <p>When a record cannot be deserialized (poison pill), the dead letter publisher forwards the
 * <i>original raw bytes</i> to the DLT. Encoding those bytes as JSON again would corrupt the
 * evidence, so they are written as-is.
 */
public class DltAwareJsonSerializer extends JsonSerializer<Object> {

    @Override
    public byte[] serialize(String topic, Headers headers, Object data) {
        if (data instanceof byte[] raw) {
            return raw;
        }
        return super.serialize(topic, headers, data);
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        if (data instanceof byte[] raw) {
            return raw;
        }
        return super.serialize(topic, data);
    }
}
