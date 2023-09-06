package io.hstream.io.standalone;

import io.hstream.HRecord;
import io.hstream.HStreamClient;
import io.hstream.ReceivedRecord;
import io.hstream.StreamShardOffset;
import io.hstream.io.*;
import io.hstream.io.impl.*;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.hstream.io.impl.spec.ReaderSpec.FROM_OFFSET_NAME;
import static io.hstream.io.impl.spec.ReaderSpec.FromOffsetEnum;

@Slf4j
public class StandaloneSinkTaskContext implements SinkTaskContext {
    HRecord cfg;
    HStreamClient client;
    CountDownLatch latch = new CountDownLatch(1);
    SinkOffsetsManager sinkOffsetsManager;
    SinkSkipStrategy sinkSkipStrategy;
    SinkRetryStrategy retryStrategy;

    boolean enableLogReport = false;
    ScheduledExecutorService logExecutor;
    AtomicLong deliveredRecords = new AtomicLong(0);
    AtomicLong deliveredBytes = new AtomicLong(0);

    @Override
    public KvStore getKvStore() {
        throw new UnsupportedOperationException("getKvStore SHOULD NOT BE CALLED");
    }

    @Override
    public ReportMessage getReportMessage() {
        throw new UnsupportedOperationException("getReportMessage SHOULD NOT BE CALLED");
    }

    @Override
    public void init(HRecord config, KvStore kv) {
        this.cfg = config;
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
        this.sinkOffsetsManager = new StandaloneSinkOffsetsManager(client, cCfg.getString("offsetStream"));
        var errorRecorder = new ErrorRecorder(client, cCfg);
        retryStrategy = new SinkRetryStrategy(cCfg);
        sinkSkipStrategy = new SinkSkipStrategyImpl(cCfg, errorRecorder);

        // log report
        if (cCfg.contains("enableLogReport")) {
            enableLogReport = cCfg.getBoolean("enableLogReport");
            if (enableLogReport) {
                setupLogReport();
            }
        }

//        var taskId = cfg.getString("task");
        var stream = cCfg.getString("stream");
        var shards = client.listShards(stream);
        if (shards.size() > 1) {
            log.warn("source stream shards > 1");
        }
        latch = new CountDownLatch(1);
        var offsets = sinkOffsetsManager.getStoredOffsets();
        log.info("offsets:{}", offsets);
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
            retryStrategy.resetRetry(batch.getShardId());
            if (enableLogReport) {
                updateMetrics(batch.getSinkRecords());
            }
        };
        for (var shard : shards) {
            StreamShardOffset offset;
            if (offsets.containsKey(shard.getShardId())) {
                offset = new StreamShardOffset(offsets.get(shard.getShardId()));
            } else {
                offset = getOffsetFromConfig(cCfg);
            }
            new Thread(() -> {
                try (var reader = client.newReader()
                        .streamName(stream)
                        .readerId("io_reader_" + UUID.randomUUID())
                        .shardId(shard.getShardId())
                        .shardOffset(offset)
                        .timeoutMs(1000)
                        .build()) {
                    BufferedSender sender = new BufferedSender(stream, shard.getShardId(), cCfg, timeFlushExecutor, innerHandler);
                    int retry = 0;
                    int maxRetry = 3;
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

    void updateMetrics(List<SinkRecord> records) {
        var bytesSize = 0;
        for (var r : records) {
            bytesSize += r.record.length;
        }
        deliveredBytes.addAndGet(bytesSize);
        deliveredRecords.addAndGet(records.size());
    }

    void setupLogReport() {
        logExecutor = Executors.newSingleThreadScheduledExecutor();
        logExecutor.scheduleAtFixedRate(() -> {
            var db = deliveredBytes.getAndSet(0);
            var dr = deliveredRecords.getAndSet(0);
            log.info("delivered bytes:{} Bytes/s, {} M/s , {} records/s", db, db / (1024 * 1024), dr);
        }, 1, 1, TimeUnit.SECONDS);
    }

    void fail() {
        // failed
        latch.countDown();
        log.info("connector failed");
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
