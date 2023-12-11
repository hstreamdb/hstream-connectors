package io.hstream.io.impl.spec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.hstream.io.Utils;
import lombok.Getter;

import java.util.List;

public class ErrorSpec implements SpecGroup {
    @Override
    public String name() {
        return "Error Handling";
    }

    @Override
    public List<SpecProperty> properties() {
        return List.of(new SkipStrategy(), new SkipCount(), new Stream(), new MaxRetries());
    }

    public enum SkipStrategyEnum {
        SkipAll,
        SkipSome,
        NeverSkip
    }

    static public String SKIP_STRATEGY = "task.error.skipStrategy";
    static public String SKIP_COUNT_NAME = "task.error.skipCount";
    static public String STREAM_NAME = "task.error.stream";
    static public String MAX_RETRIES = "task.error.maxRetries";

    @Getter
    static public class SkipStrategy implements SpecProperty {
        String name = SKIP_STRATEGY;
        String uiShowName = "Skip Strategy";
        String description = "skip strategy if records is unwritable";
        String type = "string";
        JsonNode defaultValue = new TextNode("NeverSkip");
        List<JsonNode> enumValues = List.of(new TextNode(SkipStrategyEnum.SkipAll.name()),
                new TextNode(SkipStrategyEnum.NeverSkip.name()),
                new TextNode(SkipStrategyEnum.SkipSome.name()));
    }

    @Getter
    static public class SkipCount implements SpecProperty {
        String name = SKIP_COUNT_NAME;
        String uiShowName = "skip error count";
        String type = "string";
        ObjectNode uiCondition = Utils.mapper.createObjectNode()
                .put("field", SKIP_STRATEGY)
                .put("value", SkipStrategyEnum.SkipSome.name());
        JsonNode defaultValue = new IntNode(5);
    }

    @Getter
    static public class Stream implements SpecProperty {
        String name = STREAM_NAME;
        String uiShowName = "Error Stream";
        String type = "string";
        String uiType = "stream";
        JsonNode uiOptions = Utils.mapper.createObjectNode()
                .put("allowNew", true);
    }

    @Getter
    static public class MaxRetries implements SpecProperty {
        String name = MAX_RETRIES;
        String uiShowName = "Error Max Retries";
        String type = "string";
        JsonNode defaultValue = new IntNode(3);
    }
}
