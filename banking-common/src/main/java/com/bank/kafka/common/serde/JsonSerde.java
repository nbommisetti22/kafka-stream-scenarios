package com.bank.kafka.common.serde;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;

/**
 * Minimal Jackson based {@link Serde} for Kafka Streams.
 *
 * <p>Unlike Spring's {@code JsonSerde} it never writes or reads type headers, so the wire
 * format is plain JSON that any producer/consumer (Spring Kafka, kcat, other languages)
 * can read. Wire format matches Spring's {@code JsonSerializer} configured with
 * {@code spring.json.add.type.headers=false}: ISO-8601 dates, unknown fields ignored.
 */
public class JsonSerde<T> implements Serde<T> {

    public static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private final Class<T> type;

    public JsonSerde(Class<T> type) {
        this.type = type;
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, data) -> {
            if (data == null) {
                return null;
            }
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (IOException e) {
                throw new SerializationException("Cannot serialize " + type.getSimpleName(), e);
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, bytes) -> {
            if (bytes == null) {
                return null; // tombstone
            }
            try {
                return MAPPER.readValue(bytes, type);
            } catch (IOException e) {
                throw new SerializationException("Cannot deserialize " + type.getSimpleName()
                        + " from topic " + topic, e);
            }
        };
    }
}
