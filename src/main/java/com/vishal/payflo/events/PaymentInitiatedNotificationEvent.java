package com.vishal.payflo.events;

import java.util.UUID;

public record PaymentInitiatedNotificationEvent(UUID transactionId, String payload) {
}
