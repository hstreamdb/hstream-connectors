package io.hstream;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.hstream.internal.Connector;
import io.hstream.internal.CreateConnectorRequest;
import io.hstream.internal.DeleteConnectorRequest;
import io.hstream.internal.HStreamApiGrpc;
import io.hstream.internal.ListConnectorsRequest;
import io.hstream.internal.LookupConnectorRequest;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.TestInfo;

@Slf4j
public class HStreamHelper {
  String serverHost = "127.0.0.1";
  public HStreamClient client;
  private final Map<Integer, ManagedChannel> channels = new HashMap<>();
  public HStreamService service;
  public TestInfo testInfo;

  HStreamHelper(TestInfo testInfo) throws Exception {
    this.testInfo = testInfo;
    service = new HStreamService();
    service.start();
    client = HStreamClient.builder().serviceUrl(serverHost + ":" + service.getServerPort()).build();
    System.out.println("HStreamDB started");
  }

  void writeStream(String stream, List<Record> records) {
    if (client.listStreams().stream().noneMatch(s -> s.getStreamName().equals(stream))) {
      client.createStream(stream);
    }
    try (var producer = client.newBufferedProducer().stream(stream).build()) {
      for (var record : records) {
        producer.write(record);
      }
    }
  }

  void close() throws Exception {
    service.writeLog(testInfo);
    channels.values().forEach(ManagedChannel::shutdown);
    client.close();
    service.stop();
  }

  void createConnector(String name, String sql) {
    var lookupReq = LookupConnectorRequest.newBuilder().setName(name).build();
    var lookupRes = getStub().lookupConnector(lookupReq);
    var res =
        getStub(lookupRes.getServerNode().getPort())
            .createConnector(CreateConnectorRequest.newBuilder().setSql(sql).build());
    log.info("create connector result:{}", res);
  }

  void deleteConnector(String name) {
    var lookupReq = LookupConnectorRequest.newBuilder().setName(name).build();
    var lookupRes = getStub().lookupConnector(lookupReq);
    getStub(lookupRes.getServerNode().getPort())
        .deleteConnector(DeleteConnectorRequest.newBuilder().setName(name).build());
    log.info("deleted connector:{}", name);
  }

  List<Connector> listConnectors() {
    var listReq = ListConnectorsRequest.newBuilder().build();
    return getStub().listConnectors(listReq).getConnectorsList();
  }

  HStreamApiGrpc.HStreamApiBlockingStub getStub() {
    if (channels.isEmpty()) {
      channels.put(
          service.getServerPort(),
          ManagedChannelBuilder.forAddress(serverHost, service.getServerPort())
              .usePlaintext()
              .build());
    }
    return HStreamApiGrpc.newBlockingStub(channels.entrySet().iterator().next().getValue());
  }

  HStreamApiGrpc.HStreamApiBlockingStub getStub(int port) {
    var channel = channels.get(port);
    if (channel == null) {
      channel = ManagedChannelBuilder.forAddress(serverHost, port).usePlaintext().build();
      channels.put(port, channel);
    }
    return HStreamApiGrpc.newBlockingStub(channels.get(port));
  }
}
