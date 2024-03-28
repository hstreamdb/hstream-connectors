package io.hstream;

import io.hstream.external.Jdbc;
import io.hstream.external.Sink;
import io.hstream.external.Source;
import io.hstream.external.Sqlserver;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

@Slf4j
public class Utils {
  public static ObjectMapper mapper = new ObjectMapper();
  static String mysqlRootPassword = "password";
  static String postgresPassword = "postgres";

  public static GenericContainer<?> makeMysql() {
    return new GenericContainer<>("mysql")
        .withEnv("MYSQL_ROOT_PASSWORD", mysqlRootPassword)
        .withExposedPorts(3306)
        .waitingFor(Wait.forListeningPort());
  }

  public static GenericContainer<?> makePostgresql() {
    return new GenericContainer<>("postgres")
        .withEnv("POSTGRES_PASSWORD", postgresPassword)
        .withExposedPorts(5432)
        .waitingFor(Wait.forListeningPort());
  }

  public static GenericContainer<?> makeMongodb() {
    return new GenericContainer<>("mongo")
        .withExposedPorts(27017)
        .waitingFor(Wait.forListeningPort());
  }

  public static Connection getPgConn(int port, String database) {
    Properties connectionProps = new Properties();
    connectionProps.put("user", "postgres");
    connectionProps.put("password", postgresPassword);

    try {
      var conn =
          DriverManager.getConnection(
              "jdbc:postgresql://127.0.0.1" + port + "/" + database, connectionProps);
      System.out.println("Connected to database");
      return conn;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  static List<HRecord> readStream(HStreamClient client, String stream, int count, int timeout) {
    var subId = "sub_" + UUID.randomUUID();
    client.createSubscription(
        Subscription.newBuilder().stream(stream)
            .subscription(subId)
            .offset(Subscription.SubscriptionOffset.EARLIEST)
            .build());
    var res = new LinkedList<HRecord>();
    var latch = new CountDownLatch(count);
    var consumer =
        client
            .newConsumer()
            .subscription(subId)
            .hRecordReceiver(
                (receivedHRecord, responder) -> {
                  res.add(receivedHRecord.getHRecord());
                  responder.ack();
                  latch.countDown();
                })
            .build();
    consumer.startAsync().awaitRunning();
    try {
      Assertions.assertTrue(latch.await(timeout, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    consumer.stopAsync().awaitTerminated();
    return res;
  }

  static List<Record> randomDataSet(int num) {
    return randomDataSet(num, false);
  }

  static List<Record> randomDataSet(int num, boolean multiKeys) {
    assert num > 0;
    var lst = new LinkedList<Record>();
    var rand = new Random();
    for (int i = 0; i < num; i++) {
      var rb =
          Record.newBuilder()
              .hRecord(
                  HRecord.newBuilder()
                      .put("key", HRecord.newBuilder().put("k1", i).build())
                      .put(
                          "value",
                          HRecord.newBuilder()
                              .put("k1", i)
                              .put("v1", rand.nextInt(100))
                              .put("v2", UUID.randomUUID().toString())
                              .build())
                      .build());
      if (multiKeys) {
        rb.partitionKey(String.valueOf(i));
      }
      lst.add(rb.build());
    }
    return lst;
  }

  static List<Record> randomPlainDataSet(int num) {
    assert num > 0;
    return randomDataSet(num).stream()
        .map(r -> Record.newBuilder().hRecord(r.getHRecord().getHRecord("value")).build())
        .collect(Collectors.toList());
  }

  static List<Record> randomRawJsonDataSet(int num) {
    assert num > 0;
    return randomPlainDataSet(num).stream()
        .map(
            r ->
                Record.newBuilder()
                    .rawRecord(
                        r.getHRecord().toCompactJsonString().getBytes(StandardCharsets.UTF_8))
                    .build())
        .collect(Collectors.toList());
  }

  static HArray randomDataSetWithoutKey(int num) {
    assert num > 0;
    var arrBuilder = HArray.newBuilder();
    var rand = new Random();
    for (int i = 0; i < num; i++) {
      arrBuilder.add(
          HRecord.newBuilder()
              .put("k1", i)
              .put("v1", rand.nextInt(100))
              .put("v2", UUID.randomUUID().toString())
              .build());
    }
    return arrBuilder.build();
  }

  static List<Record> randomDataSetWithBson(int num) {
    assert num > 0;
    var res = new LinkedList<Record>();
    var rand = new Random();
    for (int i = 0; i < num; i++) {
      res.add(
          Record.newBuilder()
              .hRecord(
                  HRecord.newBuilder()
                      .put("k1", HRecord.newBuilder().put("$numberLong", String.valueOf(i)).build())
                      .put(
                          "v1",
                          HRecord.newBuilder()
                              .put("$numberLong", String.valueOf(rand.nextInt(100)))
                              .build())
                      .put("v2", UUID.randomUUID().toString())
                      .build())
              .build());
    }
    return res;
  }

  static void createTableForRandomDataSet(Jdbc jdbc, String tableName) {
    jdbc.execute(
        String.format("create table %s (k1 int primary key, v1 int, v2 varchar(255))", tableName));
  }

  public static String getHostname() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }

  public static void runUntil(int maxCount, int delay, Supplier<Boolean> runner) {
    assert maxCount > 0;
    int count = 0;
    while (count < maxCount) {
      try {
        Thread.sleep(delay * 1000L);
        if (runner.get()) {
          return;
        }
        count++;
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    throw new RuntimeException("runUntil timeout, retried:" + count);
  }

  public static void testSourceFullSync(HStreamHelper helper, Source source) {
    // prepared data
    var dataSet = Utils.randomDataSetWithoutKey(10);
    var table = "t1";
    if (source instanceof Jdbc) {
      Utils.createTableForRandomDataSet((Jdbc) source, table);
      if (source instanceof Sqlserver) {
        ((Sqlserver) (source)).enableCdcForTable(table);
      }
    }
    source.writeDataSet(table, dataSet);
    // create connector
    var connectorName = "ss1";
    var stream = "stream01";
    helper.client.createConnector(
        CreateConnectorRequest.newBuilder()
            .name(connectorName)
            .type(ConnectorType.valueOf("SOURCE"))
            .target(source.getName())
            .config(source.getCreateConnectorConfig(stream, table))
            .build());
    var result = helper.client.listConnectors();
    Assertions.assertEquals(result.size(), 1);
    // check the stream
    Utils.runUntil(10, 3, () -> helper.client.listStreams().size() > 0);
    var res = Utils.readStream(helper.client, stream, 10, 30);
    Assertions.assertEquals(10, res.size());
    helper.client.deleteConnector(connectorName);
  }

  public static void testSinkFullSync(HStreamHelper helper, Sink sink) {
    testSinkFullSync(helper, sink, IORecordType.KEY_VALUE);
  }

  public enum IORecordType {
    RAW_JSON,
    PLAIN,
    KEY_VALUE,
    BSON
  }

  @SneakyThrows
  public static void testSinkFullSync(HStreamHelper helper, Sink sink, IORecordType recordType) {
    testSinkFullSync(helper, sink, recordType, false);
  }

  public static Connector testSinkFullSync(
      HStreamHelper helper, Sink sink, IORecordType recordType, boolean deleteConnector) {
    return testSinkFullSync(helper, sink, recordType, false, 1);
  }

  @SneakyThrows
  public static Connector testSinkFullSync(
      HStreamHelper helper,
      Sink sink,
      IORecordType recordType,
      boolean deleteConnector,
      int shardCount) {
    var streamName = "stream01";
    var connectorName = "sk1";
    var table = "t1";
    var count = 10;
    List<Record> ds = null;
    switch (recordType) {
      case RAW_JSON:
        ds = Utils.randomRawJsonDataSet(count);
        break;
      case PLAIN:
        ds = Utils.randomPlainDataSet(count);
        break;
      case KEY_VALUE:
        ds = Utils.randomDataSet(count, shardCount > 1);
        break;
      case BSON:
        ds = Utils.randomDataSetWithBson(count);
    }
    helper.writeStream(streamName, ds, shardCount);
    if (sink instanceof Jdbc) {
      Utils.createTableForRandomDataSet((Jdbc) sink, table);
    }
    var connector =
        helper.client.createConnector(
            CreateConnectorRequest.newBuilder()
                .name(connectorName)
                .type(ConnectorType.valueOf("SINK"))
                .target(sink.getName())
                .config(sink.getCreateConnectorConfig(streamName, table))
                .build());
    log.info("create connector:{} success", connector.getName());
    Utils.runUntil(10, 3, () -> sink.readDataSet(table).size() >= count);
    // wait and re-check
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    var dataSet = sink.readDataSet(table);
    Assertions.assertEquals(count, dataSet.size());
    // Thread.sleep(10000);
    // log.info("getConnector offsets:{}", helper.client.getConnector(connectorName).offsets);
    if (deleteConnector) {
      helper.client.deleteConnector(connectorName);
    }
    return connector;
  }
}
