/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;

public class TransformedStreamMeta extends DataStreamsMeta {
    public final DataStreamMeta streams;

    TransformedStreamMeta() {
        this.streams = new DataStreamMeta(null, null, false, Origin.GENERATED, null);
    }

    @JsonCreator
    public TransformedStreamMeta(DataStreamMeta streams) {
        this.streams = streams;
    }
}
