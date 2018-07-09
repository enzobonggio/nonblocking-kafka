package com.example.kafka.consumer;

import com.example.kafka.factory.ConsumerFactory;
import com.example.kafka.factory.ProducerRecordFactory;
import com.example.kafka.model.ExampleBody;
import com.example.kafka.model.ExampleQueueMessage;
import com.example.kafka.model.UniqueKafka;
import com.example.kafka.producer.ExampleProducer;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.function.UnaryOperator;

@Component
@Slf4j
public class ExampleConsumer {
    private static final String TOPIC = "demo-topic";
    private static final String TOPIC_10 = "demo-topic-10";

    private final ExampleProducer producer;
    private final ProducerRecordFactory factory;
    private final KafkaReceiver<String, ExampleQueueMessage<ExampleBody>> kafkaConsumer;

    public ExampleConsumer(
            ExampleProducer producer,
            ProducerRecordFactory factory,
            ConsumerFactory consumerFactory) {
        this.producer = producer;
        this.factory = factory;
        this.kafkaConsumer = consumerFactory.newReciever(TOPIC, TOPIC_10);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void doSomethingAfterStartup() {
        consumeMessages();
    }

    private void consumeMessages() {
        Flux<ReceiverRecord<String, ExampleQueueMessage<ExampleBody>>> kafkaFlux = kafkaConsumer.receive();
        kafkaFlux.transform(commitAndDelay())
                .doOnNext(r -> log.info("value: {}", r))
                .flatMap(r -> someShit(r).onErrorResume(RuntimeException.class, ex -> handlerError(ex, r)))
                .subscribe();
    }

    private static <T> UnaryOperator<Flux<ReceiverRecord<String, ExampleQueueMessage<T>>>> commitAndDelay() {
        return f -> f.doOnNext(r -> r.receiverOffset().acknowledge())
                .delayUntil(r -> {
                    long secondsToWait = ChronoUnit.SECONDS.between(LocalDateTime.now(), r.value().timeToBeConsume);
                    log.info("Seconds to wait {} - {}", secondsToWait, r.key());
                    if (r.value().retries == 0 || secondsToWait < 0) return Mono.empty();
                    return Mono.delay(Duration.ofSeconds(secondsToWait));
                });
    }

    private <T extends UniqueKafka> Mono<Void> handlerError(RuntimeException ex, ReceiverRecord<String, ExampleQueueMessage<T>> r) {
        log.error("Some error", ex);
        String topic = r.topic() + "-10";
        return producer.sendMessages(factory.produceRecord(topic, r.value().timeToBeConsume.plusSeconds(10), ++r.value().retries, r.value().body));
    }

    private Mono<Void> someShit(ReceiverRecord<String, ExampleQueueMessage<ExampleBody>> r) {
        val value = r.value();
        log.info("Value {}", value);
        if (value.getRetries() == 0)
            return Mono.error(new RuntimeException("Some db error"));
        return Mono.empty();
    }
}
