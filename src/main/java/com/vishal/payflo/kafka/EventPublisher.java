package com.vishal.payflo.kafka;

import com.vishal.payflo.kafka.events.PaymentEvent;
import com.vishal.payflo.kafka.topics.KafkaTopicResolver;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
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

        kafkaTemplate.send(topicName, key, paymentEvent);
    }

}
