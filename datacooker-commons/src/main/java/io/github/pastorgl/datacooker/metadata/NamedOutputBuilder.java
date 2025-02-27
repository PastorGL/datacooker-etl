/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import io.github.pastorgl.datacooker.data.StreamOrigin;
import io.github.pastorgl.datacooker.data.StreamType.StreamTypes;

import java.util.List;

public class NamedOutputBuilder {
    private final NamedStreamsMeta meta;

    public NamedOutputBuilder() {
        this.meta = new NamedStreamsMeta();
    }

    public NamedOutputBuilder mandatory(String name, String descr, StreamTypes type, StreamOrigin origin, List<String> ancestors) {
        meta.streams.put(name, new DataStreamMeta(descr, type, false, origin, ancestors));

        return this;
    }

    public NamedOutputBuilder optional(String name, String descr, StreamTypes type, StreamOrigin origin, List<String> ancestors) {
        meta.streams.put(name, new DataStreamMeta(descr, type, true, origin, ancestors));

        return this;
    }

    public NamedOutputBuilder generated(String name, String propName, String propDescr) {
        meta.streams.get(name).generated.put(propName, propDescr);

        return this;
    }

    public NamedStreamsMeta build() {
        return meta;
    }
}
