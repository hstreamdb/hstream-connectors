package io.hstream.io;

import java.util.function.Consumer;

public interface SinkTaskContext extends TaskContext {
    void handle(Consumer<SinkRecordBatch> handler);
    void handleParallel(Consumer<SinkRecordBatch> handler);
}
