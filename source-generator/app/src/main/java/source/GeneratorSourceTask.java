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
        log.info("schema:{}", schema);
        assert batchSize > 0;
        assert period > 0;
        var dataType = DataType.JSON;
        if (cfg.contains("type") && cfg.getString("type").equalsIgnoreCase("sequence")) {
            dataType = DataType.SEQUENCE;
        }
        var seq = 0;
        if (dataType.equals(DataType.SEQUENCE)) {
            var seqStr = kv.get("sequence").get();
            if (seqStr != null && !seqStr.isEmpty()) {
                seq = Integer.parseInt(seqStr);
            }
            generator = getSequenceGenerator(seq);
        } else {
            generator = getJsonGeneratorFromSchema(schema);
        }
        while (true) {
            if (needStop) {
                return;
            }
            Thread.sleep(period * 1000L);
            writeRecords(batchSize);
            if (dataType.equals(DataType.SEQUENCE)) {
                seq += batchSize;
                kv.set("sequence", String.valueOf(seq));
            }
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
    Supplier<Record> getJsonGeneratorFromSchema(String schemaStr) {
        var jsonFaker = new JsonFaker(schemaStr);
        return () -> {
            var jsonData = jsonFaker.generate();
            return Record.newBuilder().hRecord(HRecord.newBuilder().merge(jsonData).build()).build();
        };
    }

    Supplier<Record> getSequenceGenerator(int start) {
        AtomicInteger id = new AtomicInteger(start);
        return () -> {
            var jsonData = mapper.createObjectNode()
                    .put("id", id.getAndIncrement())
                    .put("randomInt", rnd.nextInt(1000))
                    .put("randomSmallInt", rnd.nextInt(10))
                    .put("randomDate", Instant.now().toString())
                    .toString();
            return Record.newBuilder().hRecord(HRecord.newBuilder().merge(jsonData).build()).build();
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
