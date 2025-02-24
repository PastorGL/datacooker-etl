/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import io.github.pastorgl.datacooker.data.StreamType.StreamTypes;

public class NamedInputBuilder {
    private final NamedStreamsMeta meta;

    public NamedInputBuilder() {
        this.meta = new NamedStreamsMeta();
    }

    public NamedInputBuilder mandatory(String name, String descr, StreamTypes type) {
        meta.streams.put(name, new DataStreamMeta(descr, type, false));

        return this;
    }

    public NamedInputBuilder optional(String name, String descr, StreamTypes type) {
        meta.streams.put(name, new DataStreamMeta(descr, type, true));

        return this;
    }

    public NamedStreamsMeta build() {
        return meta;
    }
}
