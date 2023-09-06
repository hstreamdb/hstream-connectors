package io.hstream.io.standalone;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.hstream.HStreamClient;
import io.hstream.Producer;
import io.hstream.Record;
import io.hstream.StreamShardOffset;
import io.hstream.io.SinkOffsetsManager;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class StandaloneSinkOffsetsManager implements SinkOffsetsManager {
    HStreamClient client;
    String stream;
    Producer producer;
    ConcurrentHashMap<Long, String> offsets = new ConcurrentHashMap<>();
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    static ObjectMapper mapper = new ObjectMapper();

    @SneakyThrows
    StandaloneSinkOffsetsManager(HStreamClient client, String stream) {
        this.client = client;
        this.stream = stream;
        Long shardId = null;
        try {
            shardId = client.listShards(stream).get(0).getShardId();
        } catch (Exception e) {
            client.createStream(stream);
        }
        this.producer = client.newProducer().stream(stream).build();
        if (shardId != null) {
            // if read tail record failed, skip
            try {
                var lastRecordId = client.getTailRecordId(stream, shardId);
                log.info("lastRecordId:{}", lastRecordId);
                var readerId = "reader_" + UUID.randomUUID().toString().replace("-", "_");
                var tailOffset = new StreamShardOffset(lastRecordId);
                try (var reader = client.newReader()
                        .readerId(readerId)
                        .shardId(shardId)
                        .shardOffset(tailOffset)
                        .streamName(stream)
                        .timeoutMs(10000)
                        .requestTimeoutMs(15000)
                        .build()) {
                    var records = reader.read(1).join();
                    if (records.size() > 0) {
                        var stored = mapper.readValue(records.get(0).getRecord().getRawRecord(),
                                new TypeReference<HashMap<Long, String>>(){});
                        log.info("stored offset:{}", stored);
                        offsets.putAll(stored);
                    }
                }
            } catch (Exception e) {
                log.warn("get last record failed", e);
            }
        }
        executor.scheduleAtFixedRate(this::storeOffsets, 1, 1, TimeUnit.SECONDS);
    }

    @Override
    public void update(long shardId, String recordId) {
        offsets.compute(shardId, (k, v) -> recordId);
    }

    @SneakyThrows
    void storeOffsets() {
        var stored = new HashMap<>(offsets);
        producer.write(Record.newBuilder().rawRecord(mapper.writeValueAsBytes(stored)).build());
    }

    @Override
    public Map<Long, String> getStoredOffsets() {
        return new HashMap<>(offsets);
    }

    @Override
    public void close() {
        executor.shutdown();
    }
}
