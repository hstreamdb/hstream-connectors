package io.hstream.io.impl.spec;

import java.util.List;

public interface SpecGroup {
    String name();
    default Boolean expand() {
        return false;
    }
    List<SpecProperty> properties();
}
