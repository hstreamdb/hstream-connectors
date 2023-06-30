package io.hstream.external;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.hstream.HArray;
import io.hstream.HRecord;
import io.hstream.Utils;
import java.util.LinkedList;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public class Mongodb implements Source, Sink {
  MongoDBContainer service;
  MongoClient client;
  MongoCollection<Document> collection;
  String dbStr = "d1";
  MongoDatabase db;

  public Mongodb() {
    // service
    service = new MongoDBContainer(DockerImageName.parse("mongo:4.0.10"));
    service.start();

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
  public String getCreateConnectorConfig(String stream, String target) {
    var hostname = Utils.getHostname();
    var cfg =
        Utils.mapper
            .createObjectNode()
            .put("hosts", hostname + ":" + service.getFirstMappedPort())
            .put("stream", stream)
            .put("database", dbStr)
            .put("collection", target)
            .toString();
    log.info("create mongodb connector config:{}", cfg);
    return cfg;
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

  @Override
  public String getName() {
    return "mongodb";
  }
}
