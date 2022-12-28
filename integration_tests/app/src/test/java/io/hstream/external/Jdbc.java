package io.hstream.external;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ListValue;
import com.google.protobuf.util.JsonFormat;
import io.hstream.HArray;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.jooq.JSONFormat;
import org.jooq.impl.DSL;

@Slf4j
public abstract class Jdbc implements Source, Sink {
  abstract Connection getConn();

  void prepareSchema(List<String> sqls) throws SQLException {
    var conn = getConn();
    var stmt = conn.createStatement();
    for (String sql : sqls) {
      stmt.execute(sql);
    }
  }

  public void execute(String sql) {
    try {
      try (var stmt = getConn().createStatement()) {
        stmt.execute(sql);
        log.info("executed sql:{}", sql);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public HArray readDataSet(String target) {
    var conn = getConn();
    var selectSql = String.format("select * from %s;", target);
    String resultSet =
        DSL.using(conn)
            .fetch(selectSql)
            .formatJSON(
                new JSONFormat().header(false).recordFormat(JSONFormat.RecordFormat.OBJECT));
    log.debug("resultSet:{}", resultSet);
    var arrBuilder = ListValue.newBuilder();
    try {
      JsonFormat.parser().merge(resultSet, arrBuilder);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
    return new HArray(arrBuilder.build());
  }

  @Override
  public void writeDataSet(String target, HArray dataSet) {
    try {
      var dataSetStr = JsonFormat.printer().print(dataSet.getDelegate());
      log.debug("writeDataSet:{}", dataSetStr);
      var conn = getConn();
      DSL.using(conn)
          .loadInto(DSL.using(conn).meta().getTables(target).get(0))
          .loadJSON(dataSetStr)
          .fieldsCorresponding()
          .execute();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
