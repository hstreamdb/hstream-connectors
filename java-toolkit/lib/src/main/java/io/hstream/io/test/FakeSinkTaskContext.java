package io.hstream.io.test;

import io.hstream.HRecord;
import io.hstream.io.*;
import lombok.SneakyThrows;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class FakeSinkTaskContext implements SinkTaskContext {
    final LinkedBlockingQueue<SinkRecordBatch> data = new LinkedBlockingQueue<>(10);
    KvStore kvStore;

    @SneakyThrows
    @Override
    public void handle(Consumer<SinkRecordBatch> handler) {
        while (true) {
            handler.accept(data.take());
        }
    }

    @SneakyThrows
    @Override
    public void handleParallel(Consumer<SinkRecordBatch> handler) {
        handle(handler);
    }

    @SneakyThrows
    public void appendRecords(List<SinkRecord> records) {
        String stream = "stream01";
        long shardId = 0;
        data.put(SinkRecordBatch.builder().stream(stream).shardId(shardId).sinkRecords(records).build());
    }

    @Override
    public void init(HRecord cfg, KvStore kvStore) {
        this.kvStore = kvStore;
    }

    @Override
    public KvStore getKvStore() {
        return kvStore;
    }

    @Override
    public ReportMessage getReportMessage() {
        return null;
    }

    @Override
    public void close() {}
}
