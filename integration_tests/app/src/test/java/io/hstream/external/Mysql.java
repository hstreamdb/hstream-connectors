package io.hstream.external;

import io.hstream.Utils;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

@Slf4j
public class Mysql extends Jdbc {
  GenericContainer<?> service;
  Connection conn;
  String db = "db1";

  public Mysql() {
    String password = "password";
    service =
        new GenericContainer<>("mysql:8.0")
            .withEnv("MYSQL_ROOT_PASSWORD", password)
            .withEnv("MYSQL_DATABASE", db)
            .withExposedPorts(3306)
            .waitingFor(Wait.forListeningPort());
    service.start();

    Properties connectionProps = new Properties();
    connectionProps.put("user", "root");
    connectionProps.put("password", password);

    Utils.runUntil(
        5,
        1,
        () -> {
          try {
            var conn =
                DriverManager.getConnection(
                    "jdbc:mysql://127.0.0.1:" + service.getFirstMappedPort() + "/" + db,
                    connectionProps);
            System.out.println("Connected to database");
            this.conn = conn;
            return true;
          } catch (SQLException e) {
            return false;
          }
        });
  }

  @Override
  Connection getConn() {
    return conn;
  }

  @Override
  public String getName() {
    return "mysql";
  }

  @Override
  public void close() {
    try {
      conn.close();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    service.stop();
  }

  @Override
  public String getCreateConnectorConfig(String stream, String target) {
    var cfg =
        Utils.mapper
            .createObjectNode()
            .put("user", "root")
            .put("password", "password")
            .put("host", Utils.getHostname())
            .put("port", service.getFirstMappedPort())
            .put("stream", stream)
            .put("database", db)
            .put("table", target)
            .toString();
    log.info("create mysql connector config:{}", cfg);
    return cfg;
  }
}
