package com.vishal.payflo.events;

import java.util.UUID;

public record PaymentFailedNotificationEvent(UUID transactionId, String payload) {
}
