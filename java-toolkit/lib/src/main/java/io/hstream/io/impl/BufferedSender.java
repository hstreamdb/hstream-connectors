package io.hstream.io.impl;

import io.hstream.HRecord;
import io.hstream.io.SinkRecord;
import io.hstream.io.SinkRecordBatch;
import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List; import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.hstream.io.impl.spec.BufferSpec.*;

public class BufferedSender {
    Consumer<SinkRecordBatch> handler;
    long batchMaxBytesSize = 0;
    long bufferSize = 0;
    final String stream;
    long shardId;
    List<SinkRecord> bufferedRecords = new LinkedList<>();
    BlockingQueue<SinkRecordBatch> queue;

    public BufferedSender(String stream, long shardId,
                          HRecord cfg, ScheduledThreadPoolExecutor executor,
                          Consumer<SinkRecordBatch> handler) {
        this.stream = stream;
        this.shardId = shardId;
        this.handler = handler;
        if (cfg.contains(BATCH_MAX_BYTES_SIZE)) {
            batchMaxBytesSize = cfg.getLong(BATCH_MAX_BYTES_SIZE);
        }
        int maxRecordAge = 0;
        if (cfg.contains(BATCH_MAX_AGE)) {
            maxRecordAge = cfg.getInt(BATCH_MAX_AGE);
        }
        if (batchMaxBytesSize > 0 && maxRecordAge > 0) {
            executor.scheduleAtFixedRate(this::flush, maxRecordAge, maxRecordAge, TimeUnit.MILLISECONDS);
        }
        if (cfg.contains(ENABLE_BACKGROUND_FLUSH)) {
            var queueSize = 10;
            if (cfg.contains("buffer.queue.size")) {
                queueSize = cfg.getInt("buffer.queue.size");
            }
            queue = new LinkedBlockingQueue<>(queueSize);
            createSenderThread();
        }
    }

    @SneakyThrows
    public void put(List<SinkRecord> records) {
        if (batchMaxBytesSize <= 0) {
            var newBatch = SinkRecordBatch.builder()
                    .stream(stream)
                    .shardId(shardId)
                    .sinkRecords(records)
                    .build();
            send(newBatch);
            return;
        }
        long size = 0;
        for (var r : records) {
            size += r.record.length;
        }
        synchronized (this) {
            if (size + bufferSize > batchMaxBytesSize) {
                flush();
            }
            bufferedRecords.addAll(records);
            bufferSize += size;
        }
    }

    @SneakyThrows
    void send(SinkRecordBatch batch) {
        if (queue != null) {
            queue.put(batch);
        } else {
            handler.accept(batch);
        }
    }

    @SneakyThrows
    public void flush() {
        synchronized (this) {
            if (bufferedRecords.isEmpty()) {
                return;
            }
            var newBatch = SinkRecordBatch.builder()
                    .stream(stream)
                    .shardId(shardId)
                    .sinkRecords(bufferedRecords)
                    .build();
            send(newBatch);
            bufferSize = 0;
            bufferedRecords = new ArrayList<>();
        }
    }

    void createSenderThread() {
        new Thread(() -> {
            while (true) {
                try {
                    var newBatch = queue.take();
                    handler.accept(newBatch);
                } catch (InterruptedException ignored) {}
            }
        }).start();
    }
}
