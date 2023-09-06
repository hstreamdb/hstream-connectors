package sink.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import io.hstream.HRecord;
import io.hstream.io.SinkTask;
import io.hstream.io.SinkTaskContext;
import io.hstream.io.Utils;
import io.hstream.io.standalone.StandaloneSinkTaskContext;
import io.hstream.io.standalone.StandAloneTaskRunner;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StandaloneElasticsearchSinkTask implements SinkTask {
    EsClient esClient;

    @Override
    public void run(HRecord cfg, SinkTaskContext ctx) {
        esClient = new EsClient(cfg);
        var index = cfg.getString("index");
        ctx.handleParallel((batch) -> {
            esClient.writeRecords(index, batch, ctx.getSinkSkipStrategy());
        });
    }

    @Override
    public JsonNode spec() {
        return Utils.getSpec(this, "/spec.json");
    }

    @Override
    public void stop() {}

    public static void main(String[] args) {
        System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog");
        log.info("setting up StandaloneElasticsearchSinkTask");
        new StandAloneTaskRunner().run(args, new StandaloneElasticsearchSinkTask(), new StandaloneSinkTaskContext());
    }
}
