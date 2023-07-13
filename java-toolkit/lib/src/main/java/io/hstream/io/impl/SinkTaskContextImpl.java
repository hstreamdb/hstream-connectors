package io.hstream.io.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Service;
import io.hstream.*;
import io.hstream.io.*;

import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

import static io.hstream.io.impl.spec.ReaderSpec.FROM_OFFSET_TYPE;
import static io.hstream.io.impl.spec.ReaderSpec.SPECIAL_OFFSET_NAME;

@Slf4j
public class SinkTaskContextImpl implements SinkTaskContext {
    static ObjectMapper mapper = new ObjectMapper();
    HRecord cfg;
    HStreamClient client;
    KvStore kv;
    AtomicInteger deliveredRecords = new AtomicInteger(0);
    AtomicInteger deliveredBytes = new AtomicInteger(0);
    CountDownLatch latch = new CountDownLatch(1);
    List<StreamShardReader> readers = new LinkedList<>();
    SinkOffsetsManager sinkOffsetsManager;
    ErrorHandler errorHandler;

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
        errorHandler = new ErrorHandler(client, cCfg);
//        var taskId = cfg.getString("task");
        var stream = cCfg.getString("stream");
        var shards = client.listShards(stream);
        if (shards.size() > 1) {
            log.warn("source stream shards > 1");
        }
        latch = new CountDownLatch(1);
        var offsets = sinkOffsetsManager.getStoredOffsets();
        for (var shard : shards) {
            StreamShardOffset offset;
            if (offsets.containsKey(shard.getShardId())) {
                offset = new StreamShardOffset(offsets.get(shard.getShardId()));
            } else {
                offset = getOffsetFromConfig(cCfg);
            }
            var reader = client.newStreamShardReader().streamName(stream).shardId(shard.getShardId())
                    .from(offset)
                    .batchReceiver(records -> {
                        var sinkRecords = records.stream().map(this::makeSinkRecord).collect(Collectors.toList());
                        var batch = SinkRecordBatch.builder()
                                .stream(stream)
                                .shardId(shard.getShardId())
                                .sinkRecords(sinkRecords)
                                .build();
                        if (parallel) {
                            handleWithRetry(handler, batch);
                        } else {
                            synchronized (handler) {
                                handleWithRetry(handler, batch);
                            }
                        }
                    }).build();
            reader.startAsync().awaitRunning();
            readers.add(reader);
        }
        latch.await();
        log.info("closing connector");
        close();
    }

    StreamShardOffset getOffsetFromConfig(HRecord cfg) {
        var offset = new StreamShardOffset(StreamShardOffset.SpecialOffset.EARLIEST);
        if (!cfg.contains(FROM_OFFSET_TYPE)) {
            return offset;
        }
        return new StreamShardOffset(StreamShardOffset.SpecialOffset.valueOf(cfg.getString(SPECIAL_OFFSET_NAME)));
    }

    @SneakyThrows
    void handleWithRetry(Consumer<SinkRecordBatch> handler, SinkRecordBatch batch) {
        int retryInterval = 5;
        int count = 0;
        while (true) {
            count++;
            try {
                handler.accept(batch);
                sinkOffsetsManager.update(batch.getShardId(), batch.getSinkRecords().get(batch.getSinkRecords().size() - 1).getRecordId());
                updateMetrics(batch.getSinkRecords());
                errorHandler.resetRetry(batch.getShardId());
                return;
            } catch (ConnectorExceptions.BaseException e){
                log.warn("delivery record failed:{}, tried:{}", e.getMessage(), count);
                var res = errorHandler.handleError(batch.getShardId(), new ConnectorExceptions.UnknownError(e.getMessage()));
                switch (res.action) {
                    case RETRY:
                        Thread.sleep(retryInterval * count * 1000L);
                        continue;
                    case SKIP:
                        return;
                    case FAIL_FAST:
                        fail();
                }
            } catch (Throwable e) {
                errorHandler.handleError(batch.getShardId(), new ConnectorExceptions.UnknownError(e.getMessage()));
                log.warn("deliver record failed:{}, retried:{}", e.getMessage(), count);
                e.printStackTrace();
                Thread.sleep(retryInterval * count * 1000L);
            }
        }
    }

    void fail() {
        // failed
        latch.countDown();
        log.info("connector failed");
        throw new RuntimeException("connector failed");
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

    @SneakyThrows
    @Override
    public void close() {
        latch.countDown();
        readers.forEach(Service::stopAsync);
        sinkOffsetsManager.close();
        client.close();
    }
}
