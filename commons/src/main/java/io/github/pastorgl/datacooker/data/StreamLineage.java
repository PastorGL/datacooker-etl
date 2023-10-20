/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.data;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.ArrayList;
import java.util.List;

public class StreamLineage {
    public final String name;
    public final String source;
    public final List<String> ancestors = new ArrayList<>();
    public final StreamOrigin origin;

    @JsonCreator
    public StreamLineage(String name, String source, StreamOrigin origin, List<String> ancestors) {
        this.name = name;
        this.source = source;
        this.origin = origin;
        this.ancestors.addAll(ancestors);
    }

    StreamLineage(String name, String source, StreamOrigin origin) {
        this.name = name;
        this.source = source;
        this.origin = origin;
    }

    @Override
    public String toString() {
        return name + " " + origin + " by " + source + (ancestors.isEmpty() ? "" : " from " + String.join(", ", ancestors));
    }
}
