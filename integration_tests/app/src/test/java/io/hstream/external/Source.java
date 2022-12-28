package io.hstream.external;

import io.hstream.HArray;

public interface Source extends External {
  String createSourceConnectorSql(String name, String stream, String target);

  void writeDataSet(String target, HArray dataSet);
}
