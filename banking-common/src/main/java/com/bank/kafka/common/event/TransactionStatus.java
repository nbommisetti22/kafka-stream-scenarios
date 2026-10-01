package com.bank.kafka.common.event;

public enum TransactionStatus {
    REQUESTED,
    VALIDATED,
    POSTED,
    REJECTED
}
