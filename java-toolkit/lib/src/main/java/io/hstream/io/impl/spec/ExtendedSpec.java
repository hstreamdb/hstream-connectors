package io.hstream.io.impl.spec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.hstream.io.Utils;
import lombok.SneakyThrows;

import java.util.List;

public class ExtendedSpec {
    @SneakyThrows
    static public String merge(JsonNode baseJson) {
        var specGroups = List.of(new ErrorSpec(), new ReaderSpec(), new BufferSpec());
        for (var group : specGroups) {
            ((ArrayNode)baseJson.get("ui:order")).add(Utils.mapper.createObjectNode()
                    .put("ui:type", "group")
                    .put("expand", group.expand())
                    .put("name", group.name()));
            for (var p : group.properties()) {
                if (p.getRequired()) {
                    ((ArrayNode)baseJson.get("required")).add(p.getName());
                }
                ((ArrayNode) baseJson.get("ui:order")).add(p.getName());
                ((ObjectNode) baseJson.get("properties")).set(p.getName(), p.toProperty());
            }
        }
        return baseJson.toString();
    }
}
