package com.bank.kafka.common.event;

public enum Severity {
    LOW,
    MEDIUM,
    HIGH,
    CRITICAL;

    public boolean opensCase() {
        return this == HIGH || this == CRITICAL;
    }
}
