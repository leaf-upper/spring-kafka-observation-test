package com.upperleaf.test;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import io.micrometer.observation.ObservationRegistry;

@SpringBootApplication
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Component
    public static class TestListener {

        private final Logger logger = LoggerFactory.getLogger(TestListener.class);

        @RetryableTopic(attempts = "3", backoff = @Backoff(value = 3000L), dltStrategy = DltStrategy.NO_DLT, retryTopicSuffix = "-retry")
        @KafkaListener(topics = "dev-test", id = "kafka-observation-test", idIsGroup = true)
        public void handleMessage(String message) {
            throw new RuntimeException();
        }
    }

    @EnableKafka
    @Configuration(proxyBeanMethods = false)
    public static class TestConfig {
        @Bean
        public ObservationRegistry observationRegistry() {
            return ObservationRegistry.create();
        }

        @Bean
        public ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>> containerCustomizer() {
            return container -> container.setRecordInterceptor(new LoggingInterceptor());
        }
    }

    public static class LoggingInterceptor implements RecordInterceptor<Object, Object> {

        private final Logger logger = LoggerFactory.getLogger(LoggingInterceptor.class);

        @Override
        public ConsumerRecord<Object, Object> intercept(ConsumerRecord<Object, Object> record, Consumer<Object, Object> consumer) {
            logger.info("intercept traceId={}", MDC.get("traceId"));
            return record;
        }

        @Override
        public void success(ConsumerRecord<Object, Object> record, Consumer<Object, Object> consumer) {
            logger.info("success traceId={}", MDC.get("traceId"));
        }

        @Override
        public void failure(ConsumerRecord<Object, Object> record, Exception exception, Consumer<Object, Object> consumer) {
            logger.info("failure traceId={}", MDC.get("traceId"));
        }

        @Override
        public void afterRecord(ConsumerRecord<Object, Object> record, Consumer<Object, Object> consumer) {
            logger.info("after traceId={}", MDC.get("traceId"));
        }
    }
}
