package io.hstream.io.impl;

import io.hstream.BufferedProducer;
import io.hstream.HRecord;
import io.hstream.HStreamClient;
import io.hstream.io.KvStore;
import io.hstream.io.ReportMessage;
import io.hstream.io.SourceRecord;
import io.hstream.io.SourceTaskContext;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class SourceTaskContextImpl implements SourceTaskContext {
    HStreamClient client;
    Map<String, BufferedProducer> producers = new HashMap<>();
    KvStore kvStore;
    AtomicLong deliveredRecords = new AtomicLong(0);
    AtomicLong deliveredBytes = new AtomicLong(0);

    @Override
    public void init(HRecord cfg, KvStore kv) {
        var hsCfg = cfg.getHRecord("hstream");
        this.kvStore = kv;
        var serviceUrl = hsCfg.getString("serviceUrl");
        if (System.getenv("HSTREAM_SERVICE_URL") != null) {
            serviceUrl = System.getenv("HSTREAM_SERVICE_URL");
        }
        client = HStreamClient.builder().serviceUrl(serviceUrl).build();
    }

    @Override
    public CompletableFuture<String> send(SourceRecord sourceRecord) {
        var stream = sourceRecord.stream;
        var producer = producers.get(stream);
        if (producer == null) {
            if (client.listStreams().stream().noneMatch(s -> stream.equals(s.getStreamName()))) {
                client.createStream(stream);
            }
            producer = client.newBufferedProducer().stream(sourceRecord.stream).build();
            producers.put(stream, producer);
        }
        var record = sourceRecord.record;
        var recordSize =  record.isRawRecord()
                ? record.getRawRecord().length
                : sourceRecord.record.getHRecord().getDelegate().getSerializedSize();
        return producer.write(sourceRecord.record).whenComplete((r, e) -> {
            if (e == null) {
                deliveredRecords.incrementAndGet();
                deliveredBytes.addAndGet(recordSize);
            } else {
                log.error("send record error:{}", e.getMessage());
            }
        });
    }

    @Override
    public KvStore getKvStore() {
        return kvStore;
    }

    @Override
    public ReportMessage getReportMessage() {
        return ReportMessage.builder()
                .deliveredRecords(deliveredRecords.getAndSet(0))
                .deliveredBytes(deliveredBytes.getAndSet(0))
                .offsets(List.of())
                .build();
    }

    @Override
    public void close() {
        producers.values().forEach(BufferedProducer::close);
        try {
            client.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
