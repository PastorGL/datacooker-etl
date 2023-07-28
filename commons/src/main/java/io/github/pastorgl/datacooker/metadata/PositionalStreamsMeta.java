/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import io.github.pastorgl.datacooker.data.StreamType;

import java.util.List;

public class PositionalStreamsMeta extends DataStreamsMeta {
    public final int positional;

    public final DataStreamMeta streams;

    PositionalStreamsMeta(int count, String descr, StreamType[] type) {
        this.positional = count;

        this.streams = new DataStreamMeta(descr, type, false);
    }

    PositionalStreamsMeta(int count, String descr, StreamType[] type, Origin origin, List<String> ancestors) {
        this.positional = count;

        this.streams = new DataStreamMeta(descr, type, false, origin, ancestors);
    }
}
