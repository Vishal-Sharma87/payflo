package com.vishal.payflo.events;

import java.util.UUID;

public record PaymentTimedOutEvent(UUID transactionId) {
}
