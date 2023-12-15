package source;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.hstream.HRecord;
import io.hstream.Record;
import io.hstream.io.*;
import io.hstream.io.impl.SourceTaskContextImpl;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

@Slf4j
public class GeneratorSourceTask implements SourceTask {
    static ObjectMapper mapper = new ObjectMapper();
    volatile Boolean needStop = false;
    Random rnd = new Random();
    SourceTaskContext ctx;
    KvStore kv;
    String stream;
    Supplier<Record> generator;
    String keyField;

    @SneakyThrows
    @Override
    public void run(HRecord cfg, SourceTaskContext ctx) {
        this.ctx = ctx;
        this.kv = ctx.getKvStore();
        this.stream = cfg.getString("stream");
        var batchSize = cfg.getInt("batchSize");
        var period = cfg.getInt("period");
        var schema = "";
        if (cfg.contains("schema")) {
            schema = cfg.getString("schema");
        }
        if (cfg.contains("keyField")) {
            keyField = cfg.getString("keyField");
        }
        log.info("schema:{}, keyField:{}", schema, keyField);
        assert batchSize > 0;
        assert period > 0;
        generator = getJsonGeneratorFromSchema(schema, keyField);
        while (true) {
            if (needStop) {
                return;
            }
            Thread.sleep(period * 1000L);
            writeRecords(batchSize);
        }
    }

    void writeRecords(int batchSize) {
        for (int i = 0; i < batchSize; i++) {
            ctx.send(new SourceRecord(stream, generator.get()));
        }
    }

    @Override
    public JsonNode spec() {
        return Utils.getSpec(this, "/spec.json");
    }

    @SneakyThrows
    Supplier<Record> getJsonGeneratorFromSchema(String schemaStr, String keyField) {
        var jsonFaker = new JsonFaker(schemaStr);
        return () -> {
            var jsonData = jsonFaker.generate();
            var hRecord = HRecord.newBuilder().merge(jsonData).build();
            String key = null;
            if (keyField != null) {
                key = Utils.pbValueToObject(hRecord.getDelegate().getFieldsMap().get(keyField)).toString();
            }
            return Record.newBuilder().partitionKey(key).hRecord(hRecord).build();
        };
    }

    @Override
    public void stop() {
        needStop = true;
    }

    public static void main(String[] args) {
        var ctx = new SourceTaskContextImpl();
        new TaskRunner().run(args, new GeneratorSourceTask(), ctx);
    }
}
