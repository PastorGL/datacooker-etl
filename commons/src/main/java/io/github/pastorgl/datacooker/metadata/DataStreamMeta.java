/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.github.pastorgl.datacooker.data.StreamType;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataStreamMeta implements Serializable {
    public final String descr;

    public final StreamType[] type;

    public final StreamOrigin origin;

    public final List<String> ancestors;

    public final Map<String, String> generated;

    public final boolean optional;

    public DataStreamMeta(String descr, StreamType[] type, boolean optional) {
        this.descr = descr;

        this.origin = null;
        this.ancestors = null;

        this.type = type;
        this.optional = optional;

        this.generated = null;
    }

    public DataStreamMeta(String descr, StreamType[] type, boolean optional, StreamOrigin origin, List<String> ancestors) {
        this.descr = descr;

        this.origin = origin;
        this.ancestors = ancestors;

        this.type = type;
        this.optional = optional;

        this.generated = (origin == StreamOrigin.FILTERED) ? null : new HashMap<>();
    }

    @JsonCreator
    public DataStreamMeta(String descr, StreamType[] type, StreamOrigin origin, List<String> ancestors, Map<String, String> generated, boolean optional) {
        this.descr = descr;
        this.type = type;
        this.origin = origin;
        this.ancestors = ancestors;
        this.generated = generated;
        this.optional = optional;
    }
}
