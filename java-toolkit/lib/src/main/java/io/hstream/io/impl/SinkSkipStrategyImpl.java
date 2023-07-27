package io.hstream.io.impl;

import io.hstream.HRecord;
import io.hstream.HStreamClient;
import io.hstream.io.ConnectorExceptions;
import io.hstream.io.SinkRecord;
import io.hstream.io.SinkRecordBatch;
import io.hstream.io.SinkSkipStrategy;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicInteger;

import static io.hstream.io.impl.spec.ErrorSpec.*;

@Slf4j
public class SinkSkipStrategyImpl implements SinkSkipStrategy {
    // skip
    int skipCount = -1;
    AtomicInteger skipped = new AtomicInteger(0);
    HStreamClient client;
    ErrorRecorder errorRecorder;

    public SinkSkipStrategyImpl(HRecord cfg, ErrorRecorder errorRecorder) {
        this.errorRecorder = errorRecorder;

        // skip count
        if (cfg.contains(SKIP_STRATEGY)) {
            switch (SkipStrategyEnum.valueOf(cfg.getString(SKIP_STRATEGY))) {
                case SkipAll:
                    skipCount = -1;
                    break;
                case NeverSkip:
                    skipCount = 0;
                    break;
                case SkipSome:
                    skipCount = cfg.getInt(SKIP_COUNT_NAME);
                    break;
            }
        }
    }

    @Override
    public boolean trySkip(SinkRecord record, String reason) {
        log.warn("handle skip batch:{}", reason);
        var result = showSkip(1);
        if (result) {
            errorRecorder.recordError(new ConnectorExceptions.InvalidSinkRecordException(record, reason));
        }
        return result;
    }

    @Override
    public boolean trySkipBatch(SinkRecordBatch batch, String reason) {
        log.warn("handle skip batch:{}", reason);
        var result = showSkip(batch.getSinkRecords().size());
        if (result) {
            errorRecorder.recordError(new ConnectorExceptions.InvalidBatchError(batch, reason));
        }
        return result;
    }

    boolean showSkip(int recordSize) {
        if (skipCount < 0) {
            return true;
        }
        skipped.addAndGet(recordSize);
        return skipCount >= skipped.get();
    }

    @SneakyThrows
    public void close() {
        client.close();
    }
}
