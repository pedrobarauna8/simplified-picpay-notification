package com.example.consumer.kafkaconsumer.infra.consumer;

import com.example.consumer.kafkaconsumer.core.provider.NotificationProvider;
import lombok.experimental.FieldDefaults;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Log4j2
@FieldDefaults(makeFinal = true, level = lombok.AccessLevel.PRIVATE)
@Service
public class KafkaConsumer {

    NotificationProvider notificationProvider;

    public KafkaConsumer(NotificationProvider notificationProvider) {
        this.notificationProvider = notificationProvider;
    }

    @KafkaListener(topics = "${topic.name.consumer}", groupId = "group_id")
    public void consume(ConsumerRecord<String, String> payload) {

        var body = payload.value();

        notificationProvider.sendNotification(body);

        log.info(body);

    }
}
