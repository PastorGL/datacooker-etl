/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.github.pastorgl.datacooker.data.StreamType;

import java.util.Map;

public class TransformMeta extends ConfigurableMeta {
    public final StreamType from;
    public final StreamType to;

    public final TransformedStreamMeta transformed;
    private final Boolean keyAfter;

    public TransformMeta(String verb, StreamType from, StreamType to, String descr, Map<String, DefinitionMeta> definitions, TransformedStreamMeta transformed) {
        super(verb, descr, definitions);

        this.from = from;
        this.to = to;

        this.transformed = transformed;

        keyAfter = null;
    }

    @JsonCreator
    public TransformMeta(String verb, StreamType from, StreamType to, String descr, Map<String, DefinitionMeta> definitions, TransformedStreamMeta transformed, Boolean keyAfter) {
        super(verb, descr, definitions);

        this.from = from;
        this.to = to;

        this.transformed = transformed;
        this.keyAfter = keyAfter;
    }

    public boolean keyAfter() {
        return (keyAfter != null) ? keyAfter : (from == StreamType.PlainText);
    }
}
