package io.hstream.io.impl.spec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import lombok.Getter;

import java.util.List;

import static io.hstream.StreamShardOffset.SpecialOffset.EARLIEST;
import static io.hstream.StreamShardOffset.SpecialOffset.LATEST;

public class ReaderSpec {
    static List<SpecProperty> properties() {
        return List.of(new FromOffset());
    }

    static public String FROM_OFFSET = "task.reader.fromOffset";

    @Getter
    static public class FromOffset implements SpecProperty {
        String name = FROM_OFFSET;
        String type = "string";
        List<JsonNode> enumValues = List.of(new TextNode(EARLIEST.toString()), new TextNode(LATEST.toString()));
        JsonNode defaultValue = new TextNode(EARLIEST.toString());
    }
}
