package io.hstream.io.impl;

import io.hstream.HRecord;
import io.hstream.HServerException;
import io.hstream.HStreamClient;
import io.hstream.Record;
import io.hstream.io.ConnectorExceptions;
import lombok.Builder;
import lombok.Getter;
import lombok.SneakyThrows;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static io.hstream.io.impl.spec.ErrorSpec.SKIP_COUNT_NAME;
import static io.hstream.io.impl.spec.ErrorSpec.STREAM_NAME;

public class ErrorHandler {
    // skip
    int skipCount = -1;
    AtomicInteger skipped = new AtomicInteger(0);

    // retry
    int maxRetries = 3;
    ConcurrentHashMap<Long, Integer> retried;

    String errorStream;
    HStreamClient client;

    @Getter
    @Builder
    public static class Result {
        Action action;
    }

    public enum Action {
        SKIP,
        RETRY,
        FAIL_FAST
    }

    public ErrorHandler(HStreamClient client, HRecord cfg) {
        if (cfg.contains(SKIP_COUNT_NAME)) {
            skipCount = cfg.getInt(SKIP_COUNT_NAME);
        }
        if (cfg.contains(STREAM_NAME)) {
            errorStream = cfg.getString(STREAM_NAME);
        } else {
            errorStream = "connector_error_stream_" + UUID.randomUUID();
        }
        this.client = client;
        try {
            client.getStream(errorStream);
        } catch (HServerException e) {
            client.createStream(errorStream);
        }
    }

    public Result handleError(long shardId, ConnectorExceptions.BaseException e) {
        // record error
        recordError(e);

        // should retry
        if (e.shouldRetry()) {
            var count = retried.getOrDefault(shardId, 0);
            if (count < maxRetries) {
                retried.put(shardId, count + 1);
                return Result.builder().action(Action.RETRY).build();
            }
        }

        // skippable
        if (skipCount < 0) {
            return Result.builder().action(Action.SKIP).build();
        }
        if (skipCount > skipped.get()) {
            skipped.incrementAndGet();
            return Result.builder().action(Action.SKIP).build();
        } else {
            return Result.builder().action(Action.FAIL_FAST).build();
        }
    }

    void recordError(ConnectorExceptions.BaseException e) {
        var producer = client.newProducer().stream(errorStream).build();
        var record = Record.newBuilder().rawRecord(e.errorRecord().getBytes(StandardCharsets.UTF_8)).build();
        producer.write(record).join();
    }

    @SneakyThrows
    public void close() {
        client.close();
    }
}
