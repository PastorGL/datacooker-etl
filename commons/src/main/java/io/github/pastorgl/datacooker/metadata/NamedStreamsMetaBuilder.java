/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import io.github.pastorgl.datacooker.data.StreamType;

import java.util.List;

public class NamedStreamsMetaBuilder {
    private final NamedStreamsMeta meta;

    public NamedStreamsMetaBuilder() {
        this.meta = new NamedStreamsMeta();
    }

    public NamedStreamsMetaBuilder mandatoryInput(String name, String descr, StreamType[] type) {
        meta.streams.put(name, new DataStreamMeta(descr, type, false));

        return this;
    }

    public NamedStreamsMetaBuilder mandatoryOutput(String name, String descr, StreamType[] type, StreamOrigin origin, List<String> ancestors) {
        meta.streams.put(name, new DataStreamMeta(descr, type, false, origin, ancestors));

        return this;
    }

    public NamedStreamsMetaBuilder optionalInput(String name, String descr, StreamType[] type) {
        meta.streams.put(name, new DataStreamMeta(descr, type, true));

        return this;
    }

    public NamedStreamsMetaBuilder optionalOutput(String name, String descr, StreamType[] type, StreamOrigin origin, List<String> ancestors) {
        meta.streams.put(name, new DataStreamMeta(descr, type, true, origin, ancestors));

        return this;
    }

    public NamedStreamsMetaBuilder generated(String name, String propName, String propDescr) {
        meta.streams.get(name).generated.put(propName, propDescr);

        return this;
    }

    public NamedStreamsMeta build() {
        return meta;
    }
}
