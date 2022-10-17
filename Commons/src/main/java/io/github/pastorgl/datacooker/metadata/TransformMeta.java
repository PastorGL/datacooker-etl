/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import io.github.pastorgl.datacooker.data.StreamType;

import java.util.Map;

public class TransformMeta {
    public final String verb;
    public final String descr;

    public final StreamType from;
    public final StreamType to;

    public final Map<String, DefinitionMeta> definitions;
    public final TransformedStreamMeta transformed;

    public TransformMeta(String verb, StreamType from, StreamType to, String descr, Map<String, DefinitionMeta> definitions, TransformedStreamMeta transformed) {
        this.verb = verb;
        this.descr = descr;

        this.from = from;
        this.to = to;

        this.definitions = definitions;
        this.transformed = transformed;
    }
}
