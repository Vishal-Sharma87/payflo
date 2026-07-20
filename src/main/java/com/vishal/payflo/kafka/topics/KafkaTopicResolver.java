package com.vishal.payflo.kafka.topics;


import org.springframework.stereotype.Component;

@Component
public class KafkaTopicResolver {

    private final KafkaTopicsProperties kafkaTopicsProperties;

    public KafkaTopicResolver(KafkaTopicsProperties kafkaTopicsProperties){
        this.kafkaTopicsProperties = kafkaTopicsProperties;
    }


    public String resolve(KafkaTopic topic) {
        return switch (topic) {
            case PAYMENT_INITIATED -> kafkaTopicsProperties.paymentInitiated();
            case PAYMENT_INITIATED_NOTIFICATION -> kafkaTopicsProperties.notificationPaymentInitiated();
            case PAYMENT_RECEIVED -> kafkaTopicsProperties.paymentReceived();
            case PAYMENT_COMPLETED_NOTIFICATION -> kafkaTopicsProperties.notificationPaymentCompleted();
            case PAYMENT_FAILED -> kafkaTopicsProperties.paymentFailed();
            case PAYMENT_FAILED_NOTIFICATION -> kafkaTopicsProperties.notificationPaymentFailed();
            case PAYMENT_TIMED_OUT -> kafkaTopicsProperties.paymentTimedOut();
            case PAYMENT_TIMED_OUT_NOTIFICATION -> kafkaTopicsProperties.notificationPaymentTimedOut();
            case DLT -> kafkaTopicsProperties.dlt();
        };
    }
}
