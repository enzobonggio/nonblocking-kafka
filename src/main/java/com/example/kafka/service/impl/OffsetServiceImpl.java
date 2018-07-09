package com.example.kafka.service.impl;

import com.example.kafka.dao.OffsetDao;
import com.example.kafka.service.OffsetService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.Optional;

@Service
public class OffsetServiceImpl implements OffsetService {

    private final OffsetDao offsetDao;

    @Autowired
    public OffsetServiceImpl(OffsetDao offsetDao) {
        this.offsetDao = offsetDao;
    }

    @Override
    public Optional<Long> getOptional(String topic, int partition) {
        return get(topic, partition).blockOptional();
    }

    @Override
    public Mono<Long> get(String topic, int partition) {
        return offsetDao.get(topic, partition);
    }
}
