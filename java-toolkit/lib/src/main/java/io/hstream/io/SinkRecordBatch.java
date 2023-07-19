package io.hstream.io;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@Builder
public class SinkRecordBatch {
    String stream;
    long shardId;
    List<SinkRecord> sinkRecords;
}
