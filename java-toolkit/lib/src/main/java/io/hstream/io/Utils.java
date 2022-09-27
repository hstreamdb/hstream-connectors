package io.hstream.io;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Objects;

public class Utils {
    public static JsonNode getSpec(Task task, String specPath) {
        try {
            return new ObjectMapper().readTree(Objects.requireNonNull(task.getClass().getResourceAsStream(specPath)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
