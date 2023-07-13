package io.hstream.io;

import lombok.Builder;
import lombok.Getter;

import java.util.List;

@Getter
@Builder
public class SinkRecordBatch {
    String stream;
    long shardId;
    List<SinkRecord> sinkRecords;
}
