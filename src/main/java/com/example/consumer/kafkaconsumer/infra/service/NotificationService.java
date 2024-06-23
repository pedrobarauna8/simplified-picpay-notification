package com.example.consumer.kafkaconsumer.infra.service;

import com.example.consumer.kafkaconsumer.core.provider.NotificationProvider;
import lombok.experimental.FieldDefaults;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.client.circuitbreaker.CircuitBreakerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

@Log4j2
@FieldDefaults(makeFinal = true, level = lombok.AccessLevel.PRIVATE)
@Service
public class NotificationService implements NotificationProvider {

    private final CircuitBreakerFactory circuitBreakerFactory;
    private final RestTemplate restTemplate;
    private final String url;

    public NotificationService(CircuitBreakerFactory circuitBreakerFactory,
                               RestTemplate restTemplate,
                               @Value("${url.notification-service}") String url) {
        this.circuitBreakerFactory = circuitBreakerFactory;
        this.restTemplate = restTemplate;
        this.url = url;
    }

    @Override
    public void sendNotification(String body) {

        var circuitBreaker = circuitBreakerFactory.create("circuitbreaker");

        var response = circuitBreaker.run(() -> restTemplate
                .postForEntity(
                        url,
                        body,
                        String.class), throwable -> HttpServerErrorException.InternalServerError.class);

        log.info(response);
    }
}
