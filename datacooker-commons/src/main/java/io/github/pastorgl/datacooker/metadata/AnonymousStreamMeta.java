/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.github.pastorgl.datacooker.data.StreamOrigin;
import io.github.pastorgl.datacooker.data.StreamType.StreamTypes;

import java.util.List;

public class AnonymousStreamMeta extends DataStreamsMeta {
    public final DataStreamMeta stream;

    AnonymousStreamMeta(String descr, StreamTypes type) {
        super(true);
        this.stream = new DataStreamMeta(descr, type, false);
    }

    AnonymousStreamMeta(String descr, StreamTypes type, StreamOrigin origin, List<String> ancestors) {
        super(true);
        this.stream = new DataStreamMeta(descr, type, false, origin, ancestors);
    }

    @JsonCreator
    public AnonymousStreamMeta(DataStreamMeta stream) {
        super(true);
        this.stream = stream;
    }
}
