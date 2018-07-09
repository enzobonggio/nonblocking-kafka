package com.example.kafka.service;

import reactor.core.publisher.Mono;

import java.util.Optional;

public interface OffsetService {
    Optional<Long> getOptional(String topic, int partition);

    Mono<Long> get(String topic, int partition);
}