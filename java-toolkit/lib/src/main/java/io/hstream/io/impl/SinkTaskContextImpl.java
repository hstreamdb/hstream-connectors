package io.hstream.io.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.hstream.*;
import io.hstream.io.*;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

import static io.hstream.io.impl.spec.ReaderSpec.*;

@Slf4j
public class SinkTaskContextImpl implements SinkTaskContext {
    static ObjectMapper mapper = new ObjectMapper();
    HRecord cfg;
    HStreamClient client;
    KvStore kv;
    AtomicLong deliveredRecords = new AtomicLong(0);
    AtomicLong deliveredBytes = new AtomicLong(0);
    CountDownLatch latch = new CountDownLatch(1);
    SinkOffsetsManager sinkOffsetsManager;
    SinkSkipStrategy sinkSkipStrategy;
    SinkRetryStrategy retryStrategy;

    @Override
    public KvStore getKvStore() {
        return kv;
    }

    @Override
    public ReportMessage getReportMessage() {
        var offsets = sinkOffsetsManager.getStoredOffsets().entrySet().stream()
                .map(entry -> (JsonNode) mapper.createObjectNode()
                        .put("shardId", entry.getKey())
                        .put("offset", entry.getValue()))
                .collect(Collectors.toList());
        return ReportMessage.builder()
                .deliveredRecords(deliveredRecords.getAndSet(0))
                .deliveredBytes(deliveredBytes.getAndSet(0))
                .offsets(offsets)
                .build();
    }

    @Override
    public void init(HRecord config, KvStore kv) {
        this.cfg = config;
        this.kv = kv;
        sinkOffsetsManager = new SinkOffsetsManagerImpl(kv, "SinkOffsetsManagerImpl");
    }

    @Override
    public void handle(Consumer<SinkRecordBatch> handler) {
        handleInternal(handler, false);
    }

    @Override
    public void handleParallel(Consumer<SinkRecordBatch> handler) {
        handleInternal(handler, true);
    }

    @SneakyThrows
    public void handleInternal(Consumer<SinkRecordBatch> handler, boolean parallel) {
        var hsCfg = cfg.getHRecord("hstream");
        var cCfg = cfg.getHRecord("connector");
        client = HStreamClient.builder().serviceUrl(hsCfg.getString("serviceUrl")).build();
        var errorRecorder = new ErrorRecorder(client, cCfg);
        retryStrategy = new SinkRetryStrategy(cCfg);
        sinkSkipStrategy = new SinkSkipStrategyImpl(cCfg, errorRecorder);
//        var taskId = cfg.getString("task");
        var stream = cCfg.getString("stream");
        var shards = client.listShards(stream);
        if (shards.size() > 1) {
            log.warn("source stream shards > 1");
        }
        latch = new CountDownLatch(1);
        var timeFlushExecutor = new ScheduledThreadPoolExecutor(4);
        // inner handler for BufferedSender
        Consumer<SinkRecordBatch> innerHandler = batch -> {
            if (parallel) {
                handleWithRetry(handler, batch);
            } else {
                synchronized (handler) {
                    handleWithRetry(handler, batch);
                }
            }
            sinkOffsetsManager.update(batch.getShardId(), batch.getSinkRecords().get(batch.getSinkRecords().size() - 1).getRecordId());
            updateMetrics(batch.getSinkRecords());
            retryStrategy.resetRetry(batch.getShardId());
        };
        for (var shard : shards) {
            new Thread(() -> {
                try {
                    BufferedSender sender = new BufferedSender(stream, shard.getShardId(), cCfg, timeFlushExecutor, innerHandler);
                    int retry = 0;
                    int maxRetry = 3;
                    var reader = getReader(stream, shard.getShardId(), cCfg);
                    while (true) {
                        try {
                            var records = reader.read(1).join();
                            if (records.size() > 0) {
                                var sinkRecords = records.stream().map(this::makeSinkRecord).collect(Collectors.toList());
                                sender.put(sinkRecords);
                            }
                            retry = 0;
                        } catch (Exception e) {
                            log.error("read records failed, retry:{}, ", retry, e);
                            retry++;
                            if (retry > maxRetry) {
                                throw new RuntimeException("retry failed");
                            }
                            Thread.sleep(retry * 3000L);
                            reader = getReader(stream, shard.getShardId(), cCfg);
                        }
                    }
                } catch (Exception e) {
                    log.error("thread for shard:{} exited", shard.getShardId(), e);
                    fail();
                }
            }).start();
        }
        latch.await();
        log.info("closing connector");
        close();
    }

    StreamShardOffset getOffset(long shardId, HRecord cfg) {
        var offsets = sinkOffsetsManager.getStoredOffsets();
        if (offsets.containsKey(shardId)) {
            return new StreamShardOffset(offsets.get(shardId));
        } else {
            return getOffsetFromConfig(cfg);
        }
    }

    Reader getReader(String stream, long shardId, HRecord cfg) {
        return client.newReader()
                .streamName(stream)
                .readerId("io_reader_" + UUID.randomUUID().toString().replace("-", "_"))
                .shardId(shardId)
                .shardOffset(getOffset(shardId, cfg))
                .timeoutMs(1000)
                .build();
    }

    StreamShardOffset getOffsetFromConfig(HRecord cfg) {
        if (cfg.contains(FROM_OFFSET_NAME)) {
            switch (FromOffsetEnum.valueOf(cfg.getString(FROM_OFFSET_NAME))) {
                case EARLIEST:
                    return new StreamShardOffset(StreamShardOffset.SpecialOffset.EARLIEST);
                case LATEST:
                    return new StreamShardOffset(StreamShardOffset.SpecialOffset.LATEST);
                default:
                    log.warn("unknown from offset:" + cfg.getString(FROM_OFFSET_NAME));
                    throw new RuntimeException("UNKNOWN from offset");
            }
        } else {
            return new StreamShardOffset(StreamShardOffset.SpecialOffset.EARLIEST);
        }
    }

    @SneakyThrows
    void handleWithRetry(Consumer<SinkRecordBatch> handler, SinkRecordBatch batch) {
        int retryInterval = 5;
        int count = 0;
        while (true) {
            count++;
            try {
                handler.accept(batch);
                return;
            } catch (ConnectorExceptions.FailFastError e){
                log.warn("fail fast error:{}", e.getMessage());
                throw e;
            } catch (Throwable e) {
                log.warn("delivery record failed:{}, tried:{}", e.getMessage(), count);
                if (!retryStrategy.showRetry(batch.getShardId(), e)) {
                    if (sinkSkipStrategy.trySkipBatch(batch, e.getMessage())) {
                        return;
                    } else {
                        fail();
                        throw e;
                    }
                }
                log.warn("retrying, retry count:{}", count);
                Thread.sleep(retryInterval * count * 1000L);
            }
        }
    }

    void fail() {
        // failed
        latch.countDown();
        log.info("connector failed");
    }

    void updateMetrics(List<SinkRecord> records) {
        var bytesSize = 0;
        for (var r : records) {
            bytesSize += r.record.length;
        }
        deliveredBytes.addAndGet(bytesSize);
        deliveredRecords.addAndGet(records.size());
    }

    SinkRecord makeSinkRecord(ReceivedRecord receivedRecord) {
        var record = receivedRecord.getRecord();
        if (record.isRawRecord()) {
            return SinkRecord.builder()
                    .record(record.getRawRecord())
                    .recordId(receivedRecord.getRecordId()).build();
        } else {
            var jsonString = record.getHRecord().toCompactJsonString();
            var formattedJson = tryFormatJsonString(jsonString);
            return SinkRecord.builder()
                    .record(formattedJson.getBytes(StandardCharsets.UTF_8))
                    .recordId(receivedRecord.getRecordId())
                    .build();
        }
    }

    String tryFormatJsonString(String str) {
        try {
            return Document.parse(str).toJson();
        } catch (Exception e) {
            return str;
        }
    }

    @Override
    public SinkSkipStrategy getSinkSkipStrategy() {
        return sinkSkipStrategy;
    }

    @SneakyThrows
    @Override
    public void close() {
        latch.countDown();
        sinkOffsetsManager.close();
        client.close();
    }
}
