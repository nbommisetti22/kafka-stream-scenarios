package com.bank.kafka.notification.gateway;

public class GatewayUnavailableException extends RuntimeException {

    public GatewayUnavailableException(String message) {
        super(message);
    }
}
