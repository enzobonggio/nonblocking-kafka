package com.example.kafka.dao;

import com.google.common.collect.ImmutableMap;
import com.rfsc.jdbc.ReactiveJdbcTemplate;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import javax.management.relation.RelationNotFoundException;
import java.util.function.Function;

@Repository
@Slf4j
public class OffsetDao {

    private final ReactiveJdbcTemplate reactiveJdbcTemplate;
    private final String sqlSearch;
    private final String sqlInsert;

    public OffsetDao(ReactiveJdbcTemplate reactiveJdbcTemplate) {
        this.reactiveJdbcTemplate = reactiveJdbcTemplate;
        this.sqlSearch = "SELECT * from kafka where topic = :topic and partition = :partition";
        this.sqlInsert = "INSERT kafka where topic = :topic and partition = :partition ON CONFLICT (topic, partition) DO UPDATE set offset = :offset";
    }

    public Mono<Long> get(String topic, int partition) {
        val params = ImmutableMap.<String, Object>builder()
                .put("topic", topic)
                .put("partition", partition)
                .build();
        return reactiveJdbcTemplate.queryForObject(sqlSearch, params, OffsetVO.class)
                .doOnNext(next -> log.info("Got information about topic {} partition {}: {}", topic, partition, next))
                .map(OffsetVO::getOffset)
                .onErrorResume(EmptyResultDataAccessException.class, handleEmptyResult(topic, partition));
    }

    public Mono<OffsetVO> put(String topic, int partition, long offset) {
        val vo = OffsetVO.builder()
                .topic(topic)
                .partition(partition)
                .offset(offset)
                .build();
        //return this.get(topic, partition);

        return reactiveJdbcTemplate.update(sqlInsert, vo)
                .handle((rows, sink) -> {
                    if (rows != 1) {
                        log.error("Could not find topic {} and partition {}", topic, partition);
                        sink.error(new RelationNotFoundException("Could no find Topic - Partition relation"));
                        return;
                    }
                    sink.next(vo);
                });

    }

    private Function<EmptyResultDataAccessException, Mono<? extends Long>> handleEmptyResult(String topic, int partition) {
        return ex -> Mono.defer(() -> {
            log.error("There is no offset for topic {} partition {}", topic, partition);
            return Mono.empty();
        });
    }

    @Data
    @Builder
    public static class OffsetVO {
        String topic;
        int partition;
        Long offset;
    }
}
