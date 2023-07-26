package io.hstream.io.impl.spec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.TextNode;
import lombok.Getter;

import java.util.List;

public class ReaderSpec {
    static List<SpecProperty> properties() {
        return List.of(new FromOffset(), new ReadTimeout());
    }

    public enum FromOffsetEnum {
        EARLIEST,
        LATEST
    }

    static public String FROM_OFFSET_NAME = "task.reader.fromOffset";
    static public String READER_TIMEOUT = "task.reader.timeout";

    @Getter
    static public class FromOffset implements SpecProperty {
        String name = FROM_OFFSET_NAME;
        String uiGroup = "reader";
        String uiShowName = "From Offset";
        String type = "string";
        List<JsonNode> enumValues = List.of(
                new TextNode(FromOffsetEnum.EARLIEST.name()),
                new TextNode(FromOffsetEnum.LATEST.name())
            );
        JsonNode defaultValue = new TextNode(FromOffsetEnum.EARLIEST.name());
    }

    @Getter
    static public class ReadTimeout implements SpecProperty {
        String name = READER_TIMEOUT;
        String uiGroup = "reader";
        String uiShowName = "Reader Timeout(unit: milliseconds)";
        String type = "string";
        JsonNode defaultValue = new IntNode(1000);
    }
}
