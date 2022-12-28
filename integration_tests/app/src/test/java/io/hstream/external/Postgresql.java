package io.hstream.external;

import io.hstream.Options;
import io.hstream.Utils;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

@Slf4j
public class Postgresql extends Jdbc {
  GenericContainer<?> service;
  Connection conn;
  String db = "db1";
  String user = "postgres";
  String password = "password";

  public Postgresql() {
    service =
        new GenericContainer<>("postgres")
            .withEnv("POSTGRES_PASSWORD", password)
            .withEnv("POSTGRES_DB", db)
            .withExposedPorts(5432)
            .withCommand("postgres", "-c", "wal_level=logical")
            .waitingFor(Wait.forListeningPort());
    service.start();

    Properties connectionProps = new Properties();
    connectionProps.put("user", user);
    connectionProps.put("password", password);

    Utils.runUntil(
        5,
        1,
        () -> {
          try {
            var conn =
                DriverManager.getConnection(
                    "jdbc:postgresql://127.0.0.1:" + service.getFirstMappedPort() + "/" + db,
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
  public void close() {
    try {
      conn.close();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    service.stop();
  }

  @Override
  public String createSinkConnectorSql(String name, String stream, String target) {
    var options =
        new Options()
            .put("user", user)
            .put("password", password)
            .put("host", Utils.getHostname())
            .put("port", service.getFirstMappedPort())
            .put("stream", stream)
            .put("database", db)
            .put("table", target);
    var sql = String.format("create sink connector %s to postgresql with (%s);", name, options);
    log.info("create sink postgresql sql:{}", sql);
    return sql;
  }

  @Override
  public String createSourceConnectorSql(String name, String stream, String target) {
    var options =
        new Options()
            .put("user", user)
            .put("password", password)
            .put("host", Utils.getHostname())
            .put("port", service.getFirstMappedPort())
            .put("stream", stream)
            .put("database", db)
            .put("table", target);
    var sql = String.format("create source connector %s from postgresql with (%s);", name, options);
    log.info("create source postgresql sql:{}", sql);
    return sql;
  }
}
