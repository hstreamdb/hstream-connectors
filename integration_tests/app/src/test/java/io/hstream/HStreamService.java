package io.hstream;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public class HStreamService {
  private final DockerImageName defaultHStreamImageName =
      DockerImageName.parse("hstreamdb/hstream:latest");

  GenericContainer<?> zk;
  GenericContainer<?> hstore;
  GenericContainer<?> server;

  private final Integer hstoreAdminPort;
  private final Integer hserverPort;
  private final Integer hserverInternalPort;

  HStreamService() throws IOException {
    zk = makeZooKeeper();
    var dataDir = Files.createTempDirectory("hstream");
    hstoreAdminPort = getRandomOpenPort();
    hserverPort = getRandomOpenPort();
    hserverInternalPort = getRandomOpenPort();
    hstore = makeHStore(dataDir, hstoreAdminPort);
    server = makeServer(dataDir, hserverPort, hserverInternalPort, hstoreAdminPort);
  }

  void start() {
    zk.start();
    hstore.start();
    server.start();
  }

  void stop() {
    server.stop();
    hstore.stop();
    zk.stop();
  }

  public GenericContainer<?> makeZooKeeper() {
    return new GenericContainer<>(DockerImageName.parse("zookeeper:3.8"))
        .withEnv("ZOO_ADMINSERVER_ENABLED", "false")
        .withNetworkMode("host");
  }

  private DockerImageName getHStreamImageName() {
    String hstreamImageName = System.getenv("HSTREAM_IMAGE_NAME");
    if (hstreamImageName == null || hstreamImageName.equals("")) {
      log.info(
          "No env variable HSTREAM_IMAGE_NAME found, use default name {}", defaultHStreamImageName);
      return defaultHStreamImageName;
    } else {
      log.info("Found env variable HSTREAM_IMAGE_NAME = {}", hstreamImageName);
      return DockerImageName.parse(hstreamImageName);
    }
  }

  public GenericContainer<?> makeHStore(Path dataDir, Integer hstoreAdminPort) {
    log.info("Start hstore with adminPort {}", hstoreAdminPort);
    return new GenericContainer<>(getHStreamImageName())
        .withNetworkMode("host")
        .withFileSystemBind(
            dataDir.toAbsolutePath().toString(), "/data/hstore", BindMode.READ_WRITE)
        .withCommand(
            "bash",
            "-c",
            "ld-dev-cluster "
                + "--root /data/hstore "
                + "--use-tcp "
                + "--tcp-host "
                + "127.0.0.1 "
                + "--user-admin-port "
                + hstoreAdminPort
                + " --no-interactive")
        .waitingFor(Wait.forLogMessage(".*LogDevice Cluster running.*", 1));
  }

  public GenericContainer<?> makeServer(
      Path dataDir, Integer hserverPort, Integer hserverInternalPort, Integer hstoreAdminPort) {
    var latestImages = getLatestImages();
    var extraImages = getExtraImages();
    return new GenericContainer<>(getHStreamImageName())
        .withNetworkMode("host")
        .withFileSystemBind(dataDir.toAbsolutePath().toString(), "/data/hstore", BindMode.READ_ONLY)
        .withFileSystemBind("/var/run/docker.sock", "/var/run/docker.sock")
        .withFileSystemBind("/tmp", "/tmp")
        .withCommand(
            "bash",
            "-c",
            " hstream-server"
                + " --bind-address 127.0.0.1"
                + " --port "
                + hserverPort
                + " --internal-port "
                + hserverInternalPort
                + " --advertised-address 127.0.0.1"
                + " --server-id 1"
                + " --seed-nodes 127.0.0.1:"
                + hserverInternalPort
                + " --metastore-uri zk://127.0.0.1:2181"
                + " --store-config /data/hstore/logdevice.conf"
                + " --store-admin-port "
                + hstoreAdminPort
                + " --log-level debug"
                + latestImages
                + extraImages
                + " --log-with-color"
                + " --store-log-level error")
        .waitingFor(Wait.forLogMessage(".*Server is started on port.*", 1));
  }

  String getLatestImages() {
    String useDefaultImages = System.getenv("HSTREAM_IO_USE_DEFAULT_IMAGES");
    if (useDefaultImages != null && useDefaultImages.equals("true")) {
      return "";
    }
    return String.join(
        " ",
        List.of(
            " --io-connector-image \"source mysql hstreamdb/source-mysql:latest\"",
            " --io-connector-image \"source postgresql hstreamdb/source-postgresql:latest\"",
            " --io-connector-image \"source sqlserver hstreamdb/source-sqlserver:latest\"",
            " --io-connector-image \"source generator hstreamdb/source-generator:latest\"",
            " --io-connector-image \"sink mysql hstreamdb/sink-mysql:latest\"",
            " --io-connector-image \"sink postgresql hstreamdb/sink-postgresql:latest\"",
            " --io-connector-image \"sink mongodb hstreamdb/sink-mongodb:latest\"",
            " --io-connector-image \"sink blackhole hstreamdb/sink-blackhole:latest\"",
            " --io-connector-image \"source mongodb hstreamdb/source-mongodb:latest\""));
  }

  String getExtraImages() {
    return "";
  }

  int getServerPort() {
    return hserverPort;
  }

  void writeLog(TestInfo testInfo) throws Exception {
    String dirFromProject =
        ".logs/"
            + testInfo.getTestClass().get().getName()
            + "/"
            + testInfo.getTestMethod().get().getName();
    log.info("log to " + dirFromProject);
    String dir = "../" + dirFromProject;
    String fileName = dir + "/server.log";
    Files.createDirectories(Path.of(dir));
    Files.writeString(Path.of(fileName), server.getLogs());
  }

  private static Integer getRandomOpenPort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      socket.setReuseAddress(true);
      return socket.getLocalPort();
    }
  }
}
