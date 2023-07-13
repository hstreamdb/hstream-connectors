package sink.mongodb;

import com.fasterxml.jackson.databind.JsonNode;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import io.hstream.HRecord;
import io.hstream.io.SinkRecord;
import io.hstream.io.SinkTask;
import io.hstream.io.SinkTaskContext;
import io.hstream.io.TaskRunner;
import io.hstream.io.Utils;
import io.hstream.io.impl.SinkTaskContextImpl;

import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

@Slf4j
public class MongodbSinkTask implements SinkTask {
    MongoClient client;
    MongoCollection<Document> collection;

    @Override
    public void run(HRecord cfg, SinkTaskContext ctx) {
        var hosts = cfg.getString("hosts");
        var dbStr = cfg.getString("database");
        var collectionStr = cfg.getString("collection");
        var authString = "";
        if (cfg.contains("user")) {
            var user = cfg.getString("user");
            var password = cfg.getString("password");
            authString = String.format("%s:%s@", user, password);
        }
        var connStr = String.format("mongodb://%s%s/", authString, hosts);
        client = MongoClients.create(connStr);
        var db = client.getDatabase(dbStr);
        collection = db.getCollection(collectionStr);
        ctx.handle((batch) -> {
            var result = collection.bulkWrite(batch.getSinkRecords().stream().map(this::mapRecord).collect(Collectors.toList()));
            log.debug("bulkWrite result:{} from steram:{}", result, batch.getStream());
        });
    }

    @Override
    public JsonNode spec() {
        return Utils.getSpec(this, "/spec.json");
    }

    @Override
    public void stop() {
        client.close();
    }

    private UpdateOneModel<Document> mapRecord(SinkRecord sinkRecord) {
        var doc = Document.parse(new String(sinkRecord.record, StandardCharsets.UTF_8));
        var keyDoc = doc.get("key", Document.class);
        var valDoc = doc.get("value");
        log.debug("keyDoc:{}, valDoc:{}", keyDoc, valDoc);
        return new UpdateOneModel<>(keyDoc,
                new Document("$set", valDoc),
                new UpdateOptions().upsert(true));
    }

    public static void main(String[] args) {
        new TaskRunner().run(args, new MongodbSinkTask(), new SinkTaskContextImpl());
    }
}
