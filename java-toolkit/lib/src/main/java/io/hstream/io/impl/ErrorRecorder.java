package io.hstream.io.impl;

import io.hstream.HRecord;
import io.hstream.HServerException;
import io.hstream.HStreamClient;
import io.hstream.Record;
import io.hstream.io.ConnectorExceptions;

import java.nio.charset.StandardCharsets;

import static io.hstream.io.impl.spec.ErrorSpec.STREAM_NAME;

public class ErrorRecorder {
    HStreamClient client;
    String errorStream;

    public ErrorRecorder(HStreamClient client, HRecord cfg) {
        // error stream
        if (cfg.contains(STREAM_NAME)) {
            errorStream = cfg.getString(STREAM_NAME);
        }

        this.client = client;
        if (errorStream != null) {
            try {
                client.getStream(errorStream);
            } catch (HServerException e) {
                client.createStream(errorStream);
            }
        }
    }

    void recordError(ConnectorExceptions.BaseException e) {
        if (errorStream == null) {
            return;
        }
        var producer = client.newProducer().stream(errorStream).build();
        var record = Record.newBuilder().rawRecord(e.errorRecord().getBytes(StandardCharsets.UTF_8)).build();
        producer.write(record).join();
    }
}
