package io.hstream.io;

import lombok.Builder;
import lombok.Getter;

import java.util.List;

@Getter
@Builder
public class SinkRecordBatch {
    long shardId;
    List<SinkRecord> sinkRecords;
}
