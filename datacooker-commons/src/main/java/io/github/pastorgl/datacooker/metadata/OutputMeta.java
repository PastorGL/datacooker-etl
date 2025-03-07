/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.github.pastorgl.datacooker.data.StreamOrigin;
import io.github.pastorgl.datacooker.data.StreamType.StreamTypes;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OutputMeta extends InputMeta {
    public final StreamOrigin origin;

    public final List<String> ancestors;

    public final Map<String, String> generated;

    OutputMeta(StreamTypes type, String descr, boolean optional) {
        super(type, descr, optional);

        this.origin = null;
        this.ancestors = null;

        this.generated = new HashMap<>();
    }

    OutputMeta(StreamTypes type, String descr, boolean optional, StreamOrigin origin, List<String> ancestors) {
        super(type, descr, optional);

        this.origin = origin;
        this.ancestors = ancestors;

        this.generated = (origin == StreamOrigin.FILTERED) ? null : new HashMap<>();
    }

    @JsonCreator
    OutputMeta(StreamTypes type, String descr, boolean optional, StreamOrigin origin, List<String> ancestors, Map<String, String> generated) {
        super(type, descr, optional);
        this.origin = origin;
        this.ancestors = ancestors;
        this.generated = generated;
    }
}
