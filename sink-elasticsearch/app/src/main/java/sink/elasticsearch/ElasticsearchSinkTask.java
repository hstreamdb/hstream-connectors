package sink.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import io.hstream.HRecord;
import io.hstream.io.*;
import io.hstream.io.impl.SinkTaskContextImpl;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ElasticsearchSinkTask implements SinkTask {
    EsClient esClient;

    @Override
    public void run(HRecord cfg, SinkTaskContext ctx) {
        esClient = new EsClient(cfg);
        var index = cfg.getString("index");
        ctx.handle((stream, records) -> {
            esClient.writeRecords(index, records);
        });
    }

    @Override
    public JsonNode spec() {
        return Utils.getSpec(this, "/spec.json");
    }

    @Override
    public void stop() {}

    public static void main(String[] args) {
        new TaskRunner().run(args, new ElasticsearchSinkTask(), new SinkTaskContextImpl());
    }
}
