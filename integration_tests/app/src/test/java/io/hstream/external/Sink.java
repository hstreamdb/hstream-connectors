package io.hstream.external;

import io.hstream.HArray;

public interface Sink extends External {
  String createSinkConnectorSql(String name, String stream, String target);

  HArray readDataSet(String target);
}
