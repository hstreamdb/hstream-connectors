package io.hstream.io.impl;

import io.hstream.HRecord;
import io.hstream.io.ConnectorExceptions;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;

import static io.hstream.io.impl.spec.ErrorSpec.*;

@Slf4j
public class SinkRetryStrategy {
    // retry
    int maxRetries = 3;
    ConcurrentHashMap<Long, Integer> retried = new ConcurrentHashMap<>();

    public SinkRetryStrategy(HRecord cfg) {
        // max retries
        if (cfg.contains(MAX_RETRIES)) {
            maxRetries = cfg.getInt(MAX_RETRIES);
        }
    }

    public boolean showRetry(long shardId, Throwable e) {
        var error = ConnectorExceptions.fromThrowable(e);
        // should retry
        if (error.shouldRetry()) {
            if (maxRetries < -1) {
                return true;
            }
            var count = retried.getOrDefault(shardId, 0);
            if (count < maxRetries) {
                retried.put(shardId, count + 1);
                return true;
            }
        }
        return false;
    }

    public void resetRetry(long shardId) {
        retried.remove(shardId);
    }
}
