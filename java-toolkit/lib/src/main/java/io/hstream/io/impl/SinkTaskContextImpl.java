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
import java.util.stream.Collectors;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

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

    @SneakyThrows
    @Override
    public void handle(BiConsumer<String, List<SinkRecord>> handler) {
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
            var offset = new StreamShardOffset(StreamShardOffset.SpecialOffset.EARLIEST);
            if (cCfg.contains("task.reader.fromOffset")) {
                offset = new StreamShardOffset(StreamShardOffset.SpecialOffset.valueOf(cCfg.getString("task.reader.fromOffset")));
            }
            if (offsets.containsKey(shard.getShardId())) {
                offset = new StreamShardOffset(offsets.get(shard.getShardId()));
            }
            var reader = client.newStreamShardReader().streamName(stream).shardId(shard.getShardId())
                    .from(offset)
                    .batchReceiver(records -> {
                        var sinkRecords = records.stream().map(this::makeSinkRecord).collect(Collectors.toList());
                        synchronized (handler) {
                            handleWithRetry(handler, stream, SinkRecordBatch.builder().shardId(shard.getShardId()).sinkRecords(sinkRecords).build());
                            sinkOffsetsManager.update(shard.getShardId(), records.get(records.size() - 1).getRecordId());
                            updateMetrics(sinkRecords);
                        }
                    }).build();
            reader.startAsync().awaitRunning();
            readers.add(reader);
        }
        latch.await();
        log.info("closing connector");
        close();
    }

    @SneakyThrows
    void handleWithRetry(BiConsumer<String, List<SinkRecord>> handler, String stream, SinkRecordBatch batch) {
        int retryInterval = 5;
        int count = 0;
        while (true) {
            count++;
            try {
                handler.accept(stream, batch.getSinkRecords());
                return;
            } catch (ConnectorExceptions.BaseException e){
                log.warn("delivery record failed:{}", e.getMessage());
                var res = errorHandler.handleError(batch.getShardId(), new ConnectorExceptions.UnknownError(e.getMessage()));
                switch (res.action) {
                    case RETRY:
                        continue;
                    case SKIP:
                        return;
                }
                break;
            } catch (Throwable e) {
                errorHandler.handleError(batch.getShardId(), new ConnectorExceptions.UnknownError(e.getMessage()));
                log.warn("deliver record failed:{}", e.getMessage());
                e.printStackTrace();
                Thread.sleep(retryInterval * count * 1000L);
            }
        }
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
