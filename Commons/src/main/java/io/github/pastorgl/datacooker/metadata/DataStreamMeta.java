/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import io.github.pastorgl.datacooker.data.StreamType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataStreamMeta {
    public final String descr;

    public final StreamType[] type;

    public final Origin origin;

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

    public DataStreamMeta(String descr, StreamType[] type, boolean optional, Origin origin, List<String> ancestors) {
        this.descr = descr;

        this.origin = origin;
        this.ancestors = ancestors;

        this.type = type;
        this.optional = optional;

        this.generated = (origin == Origin.FILTERED) ? null : new HashMap<>();
    }
}
