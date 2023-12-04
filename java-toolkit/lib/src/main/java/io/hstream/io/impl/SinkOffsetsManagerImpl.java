package io.hstream.io.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.hstream.HRecord;
import io.hstream.HStreamClient;
import io.hstream.io.KvStore;
import io.hstream.io.SinkOffsetsManager;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Slf4j
public class SinkOffsetsManagerImpl implements SinkOffsetsManager {
    KvStore kvStore;
    String offsetsKey;
    ConcurrentHashMap<Long, String> offsets = new ConcurrentHashMap<>();
    AtomicReference<HashMap<Long, String>> storedOffsets = new AtomicReference<>(new HashMap<>());
    static ObjectMapper mapper = new ObjectMapper();
    AtomicInteger bufferState = new AtomicInteger(0);
    int flushIntervalSec = 1;
    int trimIntervalSec = -1;
    HStreamClient client;
    String stream;

    SinkOffsetsManagerImpl(KvStore kvStore, String prefix, HRecord cfg, String serviceUrl) {
        this.kvStore = kvStore;
        this.offsetsKey = prefix + "_offsets";
        if (cfg.contains("offset.flush.interval.seconds")) {
            flushIntervalSec = cfg.getInt("offset.flush.interval.seconds");
        }
        init();
        new Thread(this::storeOffsets).start();
        if (cfg.contains("offset.trim.interval.seconds")) {
            trimIntervalSec = cfg.getInt("offset.trim.interval.seconds");
        }
        log.info("offset.trim.interval.seconds is {}", trimIntervalSec);
        if (trimIntervalSec > 0) {
            stream = cfg.getString("stream");
            client = HStreamClient.builder()
                    .serviceUrl(serviceUrl)
                    .requestTimeoutMs(300000)
                    .build();
            new Thread(this::trimOffsets).start();
        }
    }

    @SneakyThrows
    void init() {
        var offsetsStr = kvStore.get(offsetsKey).get();
        if (offsetsStr != null && !offsetsStr.isEmpty()) {
            var stored = mapper.readValue(offsetsStr, new TypeReference<HashMap<Long, String>>(){});
            storedOffsets.set(stored);
            offsets.putAll(stored);
        }
    }

    void trimOffsets() {
        var trimmed = new HashMap<>(storedOffsets.get());
        while (true) {
            try {
                Thread.sleep(trimIntervalSec * 1000L);
                var stored = new HashMap<>(storedOffsets.get());
                var offsets = new HashMap<Long, String>();
                for (var entry : stored.entrySet()) {
                    if (entry.getValue().equals(trimmed.getOrDefault(entry.getKey(), null))) {
                        continue;
                    }
                    offsets.put(entry.getKey(), entry.getValue());
                }
                if (!offsets.isEmpty()) {
                    log.info("trimming offsets:{}", offsets);
                    client.trimShards(stream, new ArrayList<>(offsets.values()));
                    trimmed.putAll(offsets);
                }
            } catch (Throwable e) {
                log.info("Trim Offsets failed, ", e);
            }
        }
    }

    @Override
    public void update(long shardId, String recordId) {
        offsets.compute(shardId, (k, v) -> recordId);
        bufferState.set(0);
    }

    @SneakyThrows
    void storeOffsets() {
        while (true) {
            try {
                Thread.sleep(flushIntervalSec * 1000L);
                if (bufferState.get() == 2) {
                    continue;
                }
                var stored = new HashMap<>(offsets);
                kvStore.set(offsetsKey, mapper.writeValueAsString(stored)).get();
                storedOffsets.set(stored);
                bufferState.incrementAndGet();
            } catch (Throwable e) {
                log.info("store Offsets failed, ", e);
            }
        }
    }

    @Override
    public Map<Long, String> getStoredOffsets() {
        return new HashMap<>(storedOffsets.get());
    }

    @Override
    public void close() {}
}
