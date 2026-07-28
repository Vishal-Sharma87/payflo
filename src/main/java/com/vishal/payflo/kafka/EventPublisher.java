package com.vishal.payflo.kafka;

import com.vishal.payflo.kafka.events.PaymentEvent;
import com.vishal.payflo.kafka.topics.KafkaTopicResolver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class EventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaTopicResolver kafkaTopicResolver;

    public EventPublisher(KafkaTemplate<String, Object> kafkaTemplate, KafkaTopicResolver kafkaTopicResolver){
        this.kafkaTemplate =kafkaTemplate;
        this.kafkaTopicResolver = kafkaTopicResolver;
    }


    public void publish(PaymentEvent paymentEvent){
        String key = paymentEvent.key();
        String topicName = kafkaTopicResolver.resolve(paymentEvent.topic());

        log.info("Publishing event for transactionId:{} topic:{}", key, paymentEvent.topic());
        kafkaTemplate.send(topicName, key, paymentEvent);
        log.info("Published event for transactionId:{} topic:{}", key, paymentEvent.topic());
    }

}
