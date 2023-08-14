/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.github.pastorgl.datacooker.data.StreamType;

import java.util.List;

public class PositionalStreamsMeta extends DataStreamsMeta {
    public final int count;

    public final DataStreamMeta streams;

    PositionalStreamsMeta(int count, String descr, StreamType[] type) {
        this.count = count;

        this.streams = new DataStreamMeta(descr, type, false);
    }

    PositionalStreamsMeta(int count, String descr, StreamType[] type, Origin origin, List<String> ancestors) {
        this.count = count;

        this.streams = new DataStreamMeta(descr, type, false, origin, ancestors);
    }

    @JsonCreator
    public PositionalStreamsMeta(int count, DataStreamMeta streams) {
        this.count = count;
        this.streams = streams;
    }
}
