package com.bank.kafka.common.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** Customer master data. Published to a compacted topic keyed by customerId. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CustomerProfile(
        String customerId,
        String fullName,
        String email,
        String phone,
        String homeCountry,
        String tier) {
}
