package io.hstream.io.impl;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class Utils {
    public static Map<String, String> parseStreams(String s) {
        return Arrays.stream(s.split(";"))
                .map(p -> getStreamPair(s))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static Map.Entry<String, String> getStreamPair(String s) {
        var xs = s.split(":");
        if (xs.length == 1) {
            return Map.entry(s, s);
        }
        if (xs.length == 2) {
            return Map.entry(xs[0], xs[1]);
        }
        throw new RuntimeException("invalid stream:" + s);
    }
}
