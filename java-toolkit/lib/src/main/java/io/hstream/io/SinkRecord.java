package io.hstream.io;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class SinkRecord {
    static ObjectMapper mapper = new ObjectMapper();

    public byte[] record;
    String recordId;

    JsonNode toJsonNode() {
        return mapper.createObjectNode()
                .put("recordId", recordId)
                .put("record", Utils.displayBytes(record));
    }
}
