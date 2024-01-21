package com.example.consumer.kafkaconsumer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.client.circuitbreaker.CircuitBreakerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

@Log4j2
@RequiredArgsConstructor
@Service
public class NotificationService {

    @Autowired
    private CircuitBreakerFactory circuitBreakerFactory;

    @Autowired
    private RestTemplate restTemplate;

    @Value("${topic.name.consumer")
    private String topicName;

    @Value("${url.notification-service}")
    private String url;

    @KafkaListener(topics = "${topic.name.consumer}", groupId = "group_id")
    public void consume(ConsumerRecord<String, String> payload) {

        var circuitBreaker = circuitBreakerFactory.create("circuitbreaker");

        var response = circuitBreaker.run(() -> restTemplate
                .getForEntity(
                        url,
                        String.class), throwable -> HttpServerErrorException.InternalServerError.class);

        log.info(response);
    }
}
