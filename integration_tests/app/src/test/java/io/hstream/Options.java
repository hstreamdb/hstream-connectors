package io.hstream;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class Options {
  Map<String, Object> options = new HashMap<>();

  public Options put(String key, Object val) {
    options.put(key, val);
    return this;
  }

  @Override
  public String toString() {
    return options.entrySet().stream()
        .map(o -> String.format("%s = %s", ms(o.getKey()), ms(o.getValue())))
        .collect(Collectors.joining(", "));
  }

  String ms(Object v) {
    if (v instanceof String) {
      return String.format("\"%s\"", v);
    }
    return v.toString();
  }
}
