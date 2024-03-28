package io.hstream.io.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.hstream.HRecord;
import io.hstream.HStreamClient;
import io.hstream.io.KvStore;
import io.hstream.io.SinkOffsetsManager;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class SinkOffsetsManagerImpl implements SinkOffsetsManager {
    KvStore kvStore;
    String offsetsKey;
    ConcurrentHashMap<Long, String> offsets = new ConcurrentHashMap<>();
    static ObjectMapper mapper = new ObjectMapper();
    int flushIntervalSec = 1;
    int trimIntervalSec = -1;
    HStreamClient client;
    String stream;
    // Offsets have been updated and need to commit
    private final AtomicBoolean offsetsUpdated = new AtomicBoolean(false);
    private final ScheduledExecutorService trimExecutor =  Executors.newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService offsetCommitExecutor = Executors.newSingleThreadScheduledExecutor();
    // Records have been trimmed
    private final ConcurrentHashMap<Long, String> recordsTrimmed = new ConcurrentHashMap<>();
    // Offsets have been committed to the kv store
    private final ConcurrentHashMap<Long, String> storedOffsets = new ConcurrentHashMap<>();

    SinkOffsetsManagerImpl(KvStore kvStore, String prefix, HRecord cfg, String serviceUrl) {
        this.kvStore = kvStore;
        this.offsetsKey = prefix + "_offsets";
        if (cfg.contains("offset.flush.interval.seconds")) {
            flushIntervalSec = cfg.getInt("offset.flush.interval.seconds");
        }

        init();

        offsetCommitExecutor.scheduleAtFixedRate(this::storeOffsets, flushIntervalSec, flushIntervalSec, TimeUnit.SECONDS);

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
            trimExecutor.scheduleAtFixedRate(this::trimOffsets, trimIntervalSec, trimIntervalSec, TimeUnit.SECONDS);
        }
    }

    @SneakyThrows
    void init() {
        var offsetsStr = kvStore.get(offsetsKey).get();
        if (offsetsStr != null && !offsetsStr.isEmpty()) {
            var stored = mapper.readValue(offsetsStr, new TypeReference<HashMap<Long, String>>(){});
            storedOffsets.putAll(stored);
            offsets.putAll(stored);
            recordsTrimmed.putAll(stored);
        }
    }

    void trimOffsets() {
        // compare `storedOffsets` and `trimmedRecords`, if the value of the key has been trimmed, skip it, otherwise
        // add the key and value to `recordsNeedTrim`
        var recordsNeedTrim = new HashMap<Long, String>();
        for (var entry : storedOffsets.entrySet()) {
            if(entry.getValue().equals(recordsTrimmed.getOrDefault(entry.getKey(), null))) {
                continue;
            }
            recordsNeedTrim.put(entry.getKey(), entry.getValue());
        }

        if (recordsNeedTrim.isEmpty()) {
            return;
        }

        log.info("trimming offsets:{}", recordsNeedTrim);
        try {
            client.trimShards(stream, new ArrayList<>(recordsNeedTrim.values()));
            recordsTrimmed.putAll(recordsNeedTrim);
        } catch (Throwable e) {
            log.error("trim offsets failed, ", e);
        }
    }

    @Override
    public void update(long shardId, String recordId) {
        offsets.compute(shardId, (k, v) -> recordId);
        offsetsUpdated.set(true);
    }

    void storeOffsets() {
        try {
            if (offsetsUpdated.compareAndExchange(true, false)) {
                var stored = new HashMap<>(offsets);
                kvStore.set(offsetsKey, mapper.writeValueAsString(stored)).get();
                storedOffsets.putAll(stored);
            }
        } catch (Throwable e) {
            log.info("store Offsets failed, ", e);
        }
    }

    @Override
    public Map<Long, String> getStoredOffsets() {
        return new HashMap<>(storedOffsets);
    }

    @Override
    public void close() {
        trimExecutor.shutdown();
        offsetCommitExecutor.shutdown();

        // commit offsets and do final trim before close
        storeOffsets();
        trimOffsets();
    }
}
