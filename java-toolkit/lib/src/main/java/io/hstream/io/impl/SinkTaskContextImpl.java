package io.hstream.io.impl;

import io.hstream.Consumer;
import io.hstream.HRecord;
import io.hstream.HStreamClient;
import io.hstream.Subscription;
import io.hstream.io.KvStore;
import io.hstream.io.ReportMessage;
import io.hstream.io.SinkRecord;
import io.hstream.io.SinkTaskContext;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SinkTaskContextImpl implements SinkTaskContext {
    HRecord cfg;
    HStreamClient client;
    Consumer consumer;
    KvStore kv;
    AtomicInteger deliveredRecords = new AtomicInteger(0);
    AtomicInteger deliveredBytes = new AtomicInteger(0);

    @Override
    public KvStore getKvStore() {
        return kv;
    }

    @Override
    public ReportMessage getReportMessage() {
        return ReportMessage.builder()
                .deliveredRecords(deliveredRecords.get())
                .deliveredBytes(deliveredBytes.get())
                .build();
    }

    @Override
    public void init(HRecord config, KvStore kv) {
        this.cfg = config;
        this.kv = kv;
    }

    private SinkRecord makeSinkRecord(HRecord record) {
        return new SinkRecord(record);
    }

    @Override
    public void handle(BiConsumer<String, List<SinkRecord>> handler) {
        var hsCfg = cfg.getHRecord("hstream");
        client = HStreamClient.builder().serviceUrl(hsCfg.getString("serviceUrl")).build();
        var taskId = cfg.getString("task");
        var cCfg = cfg.getHRecord("connector");
        var stream = cCfg.getString("stream");
        var subId = kv.get("hstream_subscription_id").join();
        if (subId == null) {
            subId = "connector_sub_" + taskId;
            var sub = Subscription.newBuilder().
                    stream(stream)
                    .subscription(subId)
                    .offset(Subscription.SubscriptionOffset.EARLIEST)
                    .build();
            client.createSubscription(sub);
            kv.set("hstream_subscription_id", subId).join();
        }
        this.consumer = client.newConsumer()
                .subscription(subId)
                .rawRecordReceiver(((receivedRawRecord, responder) -> {
                    log.debug("received raw record:{}", receivedRawRecord.getRecordId());
                    var hRecord = tryConvertToHRecord(receivedRawRecord.getRawRecord());
                    if (hRecord != null) {
                        handler.accept(stream, List.of(makeSinkRecord(hRecord)));
                        responder.ack();
                    }
                    deliveredRecords.incrementAndGet();
                    deliveredBytes.addAndGet(receivedRawRecord.getRawRecord().length);
                }))
                .hRecordReceiver((receivedHRecord, responder) -> {
                    log.debug("received:{}", receivedHRecord.getHRecord().toJsonString());
                    handler.accept(stream, List.of(makeSinkRecord(receivedHRecord.getHRecord())));
                    responder.ack();
                    deliveredRecords.incrementAndGet();
                    deliveredBytes.addAndGet(receivedHRecord.getHRecord().getDelegate().getSerializedSize());
                })
                .build();
        consumer.startAsync().awaitRunning();
        consumer.awaitTerminated();
    }

    HRecord tryConvertToHRecord(byte[] rawRecord) {
        try {
            return HRecord.newBuilder().merge(new String(rawRecord)).build();
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public void close() {
        try {
            consumer.stopAsync().awaitTerminated();
            client.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
