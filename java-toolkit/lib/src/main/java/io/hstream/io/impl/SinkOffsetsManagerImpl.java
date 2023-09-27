package io.hstream.io.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.hstream.io.KvStore;
import io.hstream.io.SinkOffsetsManager;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class SinkOffsetsManagerImpl implements SinkOffsetsManager {
    KvStore kvStore;
    String offsetsKey;
    ConcurrentHashMap<Long, String> offsets = new ConcurrentHashMap<>();
    AtomicReference<HashMap<Long, String>> storedOffsets = new AtomicReference<>(new HashMap<>());
    static ObjectMapper mapper = new ObjectMapper();
    AtomicInteger bufferState = new AtomicInteger(0);

    SinkOffsetsManagerImpl(KvStore kvStore, String prefix) {
        this.kvStore = kvStore;
        this.offsetsKey = prefix + "_offsets";
        init();
        new Thread(this::storeOffsets).start();
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

    @Override
    public void update(long shardId, String recordId) {
        offsets.compute(shardId, (k, v) -> recordId);
        bufferState.set(0);
    }

    @SneakyThrows
    void storeOffsets() {
        while (true) {
            try {
                Thread.sleep(1000);
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
