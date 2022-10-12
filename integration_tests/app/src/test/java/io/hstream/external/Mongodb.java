package io.hstream.external;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.hstream.HArray;
import io.hstream.HRecord;
import io.hstream.Options;
import io.hstream.Utils;
import java.io.IOException;
import java.util.LinkedList;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public class Mongodb implements Source, Sink{
    MongoDBContainer service;
    MongoClient client;
    MongoCollection<Document> collection;
    String dbStr = "d1";
    MongoDatabase db;

    public Mongodb() {
        // service
        service = new MongoDBContainer(DockerImageName.parse("mongo:4.0.10"));
        service.start();
//        try {
//            service.execInContainer("mongo --eval 'rs.initiate({_id: \"hstream_io\", members:[{_id: 0, host: \"localhost:27017\"}]})'");
//        } catch (IOException | InterruptedException e) {
//            throw new RuntimeException(e);
//        }

        // connector
        var hosts = "127.0.0.1:" + service.getFirstMappedPort();
        var authString = "";
        var connStr = String.format("mongodb://%s%s/", authString, hosts);
        client = MongoClients.create(connStr);
        db = client.getDatabase(dbStr);
        log.info("set up environment");
    }

    @Override
    public void close() {
        client.close();
        service.close();
    }

    @Override
    public String createSinkConnectorSql(String name, String stream, String target) {
        var hostname = Utils.getHostname();
        var options = new Options()
                .put("hosts", hostname + ":" + service.getFirstMappedPort())
                .put("stream", stream)
                .put("database", dbStr)
                .put("collection", target);
        var sql = String.format("create sink connector %s to mongodb with (%s);", name, options);
        log.info("create sink mongodb sql:{}", sql);
        return sql;
    }

    @Override
    public String createSourceConnectorSql(String name, String stream, String target) {
        var hostname = Utils.getHostname();
        var options = new Options()
                .put("hosts", hostname + ":" + service.getFirstMappedPort())
                .put("stream", stream)
                .put("database", dbStr)
                .put("collection", target);
        var sql = String.format("create source connector %s from mongodb with (%s);", name, options);
        log.info("create source mongodb sql:{}", sql);
        return sql;
    }

    @Override
    public HArray readDataSet(String target) {
        collection = db.getCollection(target);
        var arr = HArray.newBuilder();
        try (var cursor = collection.find().iterator()) {
            while (cursor.hasNext()) {
                var json = cursor.next().toJson();
                arr.add(HRecord.newBuilder().merge(json).build());
            }
        }
        return arr.build();
    }

    @Override
    public void writeDataSet(String target, HArray dataSet) {
        collection = db.getCollection(target);
        var docs = new LinkedList<Document>();
        for (int i = 0; i < dataSet.size(); i++) {
            var json = dataSet.getHRecord(i).toCompactJsonString();
            docs.add(Document.parse(json));
        }
        collection.insertMany(docs);
    }
}
