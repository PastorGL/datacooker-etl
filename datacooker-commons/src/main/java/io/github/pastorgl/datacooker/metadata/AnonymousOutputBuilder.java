/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import io.github.pastorgl.datacooker.data.StreamOrigin;
import io.github.pastorgl.datacooker.data.StreamType.StreamTypes;

import java.util.List;
import java.util.Map;

public class AnonymousOutputBuilder {
    private final AnonymousStreamMeta meta;

    public AnonymousOutputBuilder(String descr, StreamTypes type, StreamOrigin origin, List<String> ancestors) {
        meta = new AnonymousStreamMeta(descr, type, origin, ancestors);
    }

    public AnonymousOutputBuilder generated(String propName, String propDescr) {
        if (meta.stream.origin == StreamOrigin.FILTERED) {
            throw new IllegalArgumentException("Invalid stream origin");
        } else {
            meta.stream.generated.put(propName, propDescr);
        }

        return this;
    }

    public AnonymousOutputBuilder generated(Map<String, String> genProps) {
        if (meta.stream.origin == StreamOrigin.FILTERED) {
            throw new IllegalArgumentException("Invalid stream origin");
        } else {
            meta.stream.generated.putAll(genProps);
        }

        return this;
    }

    public AnonymousStreamMeta build() {
        return meta;
    }
}
