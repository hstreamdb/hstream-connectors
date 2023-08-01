package io.hstream.io.impl.spec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import lombok.Getter;

import java.util.List;

public class ReaderSpec implements SpecGroup {
    @Override
    public List<SpecProperty> properties() {
        return List.of(new FromOffset());
    }

    @Override
    public String name() {
        return "Reader";
    }

    public enum FromOffsetEnum {
        EARLIEST,
        LATEST
    }

    static public String FROM_OFFSET_NAME = "task.reader.fromOffset";

    @Getter
    static public class FromOffset implements SpecProperty {
        String name = FROM_OFFSET_NAME;
        String uiShowName = "From Offset";
        String type = "string";
        List<JsonNode> enumValues = List.of(
                new TextNode(FromOffsetEnum.EARLIEST.name()),
                new TextNode(FromOffsetEnum.LATEST.name())
            );
        JsonNode defaultValue = new TextNode(FromOffsetEnum.EARLIEST.name());
    }
}
