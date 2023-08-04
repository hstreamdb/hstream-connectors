package io.hstream.io.impl.spec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.hstream.io.Utils;

import java.util.List;

public interface SpecProperty {

    String getName();
    String getType();
    default Boolean getRequired() {
        return false;
    }
    default String getDescription() {
        return null;
    }
    default String getUiType() {
        return null;
    }
    default String getUiShowName() {
        return null;
    }
    default ObjectNode getUiCondition() {
        return null;
    }
    default JsonNode getDefaultValue() {
        return null;
    }
    default List<JsonNode> getEnumValues() {
        return null;
    }
    default JsonNode getUiOptions() {
        return null;
    }

    default JsonNode toProperty() {
        var obj = Utils.mapper.createObjectNode().put("type", getType());
        if (getDescription() != null) {
            obj.put("description", getDescription());
        }
        if (getUiType() != null) {
            obj.put("ui:type", getUiType());
        }
        if (getUiShowName() != null) {
            obj.put("ui:showName", getUiShowName());
        }
        if (getUiCondition()  != null) {
            obj.set("ui:condition", getUiCondition());
        }
        if (getDefaultValue() != null) {
            obj.set("default", getDefaultValue());
        }
        if (getEnumValues() != null) {
            obj.set("enum", Utils.mapper.valueToTree(getEnumValues()));
        }
        if (getUiOptions() != null) {
            obj.set("ui:options", getUiOptions());
        }
        return obj;
    }
}
