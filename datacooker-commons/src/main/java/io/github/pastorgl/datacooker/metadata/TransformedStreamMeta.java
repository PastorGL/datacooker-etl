/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.github.pastorgl.datacooker.data.StreamOrigin;

public class TransformedStreamMeta extends DataStreamsMeta {
    public final DataStreamMeta stream;

    TransformedStreamMeta() {
        super(true);
        this.stream = new DataStreamMeta(null, null, false, StreamOrigin.GENERATED, null);
    }

    @JsonCreator
    public TransformedStreamMeta(DataStreamMeta stream) {
        super(true);
        this.stream = stream;
    }
}
