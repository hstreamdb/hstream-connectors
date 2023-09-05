package io.hstream.io.standalone;

import com.google.common.util.concurrent.Service;
import io.hstream.*;
import io.hstream.io.*;
import io.hstream.io.impl.*;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Slf4j
public class StandAloneSinkTaskContext implements SinkTaskContext {
    HRecord cfg;
    HStreamClient client;
    KvStore kv;
    CountDownLatch latch = new CountDownLatch(1);
    SinkOffsetsManager sinkOffsetsManager;
    SinkSkipStrategy sinkSkipStrategy;
    SinkRetryStrategy retryStrategy;
    final Map<Long, List<BatchAckResponder>> responders = new HashMap<>();

    @Override
    public KvStore getKvStore() {
        throw new UnsupportedOperationException("SHOULD NOT BE CALLED");
    }

    @Override
    public ReportMessage getReportMessage() {
        throw new UnsupportedOperationException("SHOULD NOT BE CALLED");
    }

    @Override
    public void init(HRecord config, KvStore kv) {
        this.cfg = config;
        this.kv = kv;
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
        var stream = cCfg.getString("stream");
        String sub;
        if (cCfg.contains("subscription")) {
            sub = cCfg.getString("subscription");
        } else {
            log.error("subscription name not found in config");
            throw new RuntimeException("subscription name not found in config");
        }
        try {
            client.getSubscription(sub);
        } catch (Exception e) {
            log.info("get subscription:{}", e.getMessage());
            client.createSubscription(Subscription.newBuilder()
                            .stream(stream)
                            .subscription(sub)
                            .offset(Subscription.SubscriptionOffset.EARLIEST)
                    .build()
            );
        }
        var senders = new ConcurrentHashMap<Long, BufferedSender>();
        var executor = new ScheduledThreadPoolExecutor(4);
        var consumer = client.newConsumer()
                .name("consumer_" + UUID.randomUUID().toString().replace("-", "_"))
                .subscription(sub)
                .batchReceiver((records, batchAckResponder) -> {
                    var sinkRecords = records.stream().map(this::makeSinkRecord).collect(Collectors.toList());
                    if (sinkRecords.isEmpty()) {
                        log.warn("received empty sink records");
                        return;
                    }
                    var shardId = shardIdFromRecordId(records.get(0).getRecordId());
                    // MUST NOT ENABLE BACKGROUND FLUSH
                    var sender = senders.computeIfAbsent(shardId,
                            k -> new BufferedSender(stream, k, cCfg, executor, handlerWithRetry(handler)));
                    synchronized (responders) {
                        var responderList = responders.computeIfAbsent(shardId, k -> new LinkedList<>());
                        responderList.add(batchAckResponder);
                    }
                    sender.put(sinkRecords);
                })
                .build();
        latch = new CountDownLatch(1);
        consumer.addListener(new Service.Listener() {
            @Override
            public void failed(Service.State from, Throwable failure) {
                log.error("consumer failed", failure);
                latch.countDown();
            }}, executor);
        consumer.startAsync().awaitRunning();
        latch.await();
        log.info("closing connector");
        consumer.stopAsync();
        close();
    }

    long shardIdFromRecordId(String recordId) {
        return Long.parseLong(recordId.split("-")[0]);
    }

    Consumer<SinkRecordBatch> handlerWithRetry(Consumer<SinkRecordBatch> handler) {
        return batch -> handleWithRetry(handler, batch);
    }

    @SneakyThrows
    void handleWithRetry(Consumer<SinkRecordBatch> handler, SinkRecordBatch batch) {
        int retryInterval = 5;
        int count = 0;
        while (true) {
            count++;
            try {
                handler.accept(batch);
                synchronized (responders) {
                    var responderList = responders.get(batch.getShardId());
                    responderList.forEach(BatchAckResponder::ackAll);
                    responderList.clear();
                }
                return;
            } catch (ConnectorExceptions.FailFastError e){
                log.warn("fail fast error:{}", e.getMessage());
                throw e;
            } catch (Throwable e) {
                log.warn("delivery record failed:{}, tried:{}", e.getMessage(), count);
                if (count > 3) {
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
