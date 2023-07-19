package io.hstream.io.impl;

import io.hstream.HRecord;
import io.hstream.io.SinkRecord;
import io.hstream.io.SinkRecordBatch;
import lombok.SneakyThrows;

import java.util.LinkedList;
import java.util.List;
//import java.util.concurrent.BlockingQueue;
//import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class BufferedSender {
    Consumer<SinkRecordBatch> handler;
    long maxBufferSize = 1048576;
    long bufferSize = 0;
    final SinkRecordBatch batch;
//    BlockingQueue<SinkRecordBatch> queue = new LinkedBlockingQueue<>(2);

    public BufferedSender(String stream, long shardId,
                          HRecord cfg, ScheduledThreadPoolExecutor executor,
                          Consumer<SinkRecordBatch> handler) {
        this.batch = SinkRecordBatch.builder().
                stream(stream)
                .shardId(shardId)
                .sinkRecords(new LinkedList<>())
                .build();
        this.handler = handler;
        int maxRecordAge = 10;
        executor.scheduleAtFixedRate(this::flush, maxRecordAge, maxRecordAge, TimeUnit.SECONDS);
//        createSenderThread();
    }

    public void put(List<SinkRecord> records) {
        long size = 0;
        for (var r : records) {
            size += r.record.length;
        }
        synchronized (batch) {
            if (size + bufferSize > maxBufferSize) {
                flush();
            }
            batch.getSinkRecords().addAll(records);
            bufferSize += size;
        }
    }

    @SneakyThrows
    public void flush() {
        synchronized (batch) {
            if (batch.getSinkRecords().isEmpty()) {
                return;
            }
            var newBatch = SinkRecordBatch.builder()
                    .stream(batch.getStream())
                    .shardId(batch.getShardId())
                    .sinkRecords(batch.getSinkRecords())
                    .build();
            handler.accept(newBatch);
//            queue.put(newBatch);
            bufferSize = 0;
            batch.setSinkRecords(new LinkedList<>());
        }
    }
//
//    void createSenderThread() {
//        new Thread(() -> {
//            while (true) {
//                try {
//                    var newBatch = queue.take();
//                    handler.accept(newBatch);
//                } catch (InterruptedException ignored) {}
//            }
//        }).start();
//    }
}
