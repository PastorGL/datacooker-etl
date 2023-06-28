/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import io.github.pastorgl.datacooker.data.StreamType;

import java.util.Map;

public abstract class AdapterMeta extends ConfigurableMeta {
    public final StreamType[] type;
    public final String path;

    public AdapterMeta(String verb, String descr, String pathDescr, StreamType[] type, Map<String, DefinitionMeta> meta) {
        super(verb, descr, meta);

        this.type = type;
        this.path = pathDescr;
    }
}
