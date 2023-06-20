package source;

import lombok.SneakyThrows;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

public class JsonFaker {
    Path schemaPath = Path.of("/json/schema.json");
    Path outputPath = Path.of("/json/output.json");

    @SneakyThrows
    public JsonFaker(String schema) {
        Files.writeString(schemaPath, schema);
    }

    @SneakyThrows
    void updateOutput() {
        var process = new ProcessBuilder()
                .directory(new File("/json"))
                .command("/usr/bin/bash", "-c", "generate-json schema.json output.json")
                .inheritIO()
                .start();
        process.waitFor(3, TimeUnit.SECONDS);
    }

    @SneakyThrows
    public String generate() {
        updateOutput();
        return Files.readString(outputPath);
    }
}